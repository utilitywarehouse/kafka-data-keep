package backup

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
)

func init() {
	// Enable debug logging for slog
	slog.SetDefault(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
}

const (
	minioRegion = "us-east-1"
	bucketName  = "test-backup-bucket"
	s3User      = "uwadmin"
	s3pass      = "uwadminpass"
)

func TestBackupIntegration(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx := context.Background()

	kafkaBrokers, tkf := startKafkaService(t, ctx)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := startS3Service(t, ctx)
	t.Cleanup(ts3f)

	setupEnvS3Access()
	s3Client := newS3Client(t, ctx, s3Endpoint)

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // set the partitions manually on produce
	)
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)

	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	t.Run("multiple batches per partitions", func(t *testing.T) {
		t.Parallel()

		topic1 := "multiple-1"
		topic2 := "multiple-2"
		_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic1)
		require.NoError(t, err)

		_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic2)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("test-backup-group")
		s3Prefix := "multiple-batches/"
		cfg := AppConfig{
			Brokers:     kafkaBrokers,
			TopicsRegex: "multiple-.*",
			GroupID:     groupID,
			MinFileSize: 5000,
			WorkingDir:  workingDir,
			S3Bucket:    bucketName,
			S3Prefix:    s3Prefix,
			S3Endpoint:  s3Endpoint,
			S3Region:    minioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)

		// First batch of records
		writeRecords(t, ctx, adminClient, topic1, 0, 10, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic1, 1, 20, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 0, 20, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 1, 30, cfg.MinFileSize)
		require.NoError(t, adminClient.Flush(ctx))

		// Wait until these records are consumed
		waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 30, topic2: 50})

		// Second batch of records
		writeRecords(t, ctx, adminClient, topic1, 0, 20, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic1, 1, 10, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 0, 30, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 1, 40, cfg.MinFileSize)
		require.NoError(t, adminClient.Flush(ctx))

		// Wait until the second batch is consumed
		waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 60, topic2: 120})

		stopApp(ctx, t, cancel, errCh)

		expectedFiles := map[string]int{
			"multiple-batches/multiple-1/0/multiple-1-0-0000000000000000000.avro": 10,
			"multiple-batches/multiple-1/0/multiple-1-0-0000000000000000010.avro": 20,
			"multiple-batches/multiple-1/1/multiple-1-1-0000000000000000000.avro": 20,
			"multiple-batches/multiple-1/1/multiple-1-1-0000000000000000020.avro": 10,
			"multiple-batches/multiple-2/0/multiple-2-0-0000000000000000000.avro": 20,
			"multiple-batches/multiple-2/0/multiple-2-0-0000000000000000020.avro": 30,
			"multiple-batches/multiple-2/1/multiple-2-1-0000000000000000000.avro": 30,
			"multiple-batches/multiple-2/1/multiple-2-1-0000000000000000030.avro": 40,
		}

		filesFound := listFilesOnBucket(ctx, t, s3Client, s3Prefix)

		require.Equal(t, expectedFiles, filesFound)
	})

	t.Run("flush on stopping the app", func(t *testing.T) {
		t.Parallel()

		topic3 := "flush-stop-1"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic3)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("test-backup-group")
		s3Prefix := "flush-on-stop/"
		cfg := AppConfig{
			Brokers:            kafkaBrokers,
			TopicsRegex:        "flush-stop-.*",
			ExcludeTopicsRegex: "multiple-.*", // eclude the topics from the previous test
			GroupID:            groupID,
			MinFileSize:        100 * 1024 * 1024, // use a big limit, so we make sure we flush only on stopping the app
			WorkingDir:         workingDir,
			S3Bucket:           bucketName,
			S3Prefix:           s3Prefix,
			S3Endpoint:         s3Endpoint,
			S3Region:           minioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// write records continuously, but offsets won't be committed, since the file size limit is very high
		for i := range 10 {
			writeRecords(t, ctx, adminClient, topic3, 0, 1000, 1000)
			require.NoError(t, adminClient.Flush(ctx))
			t.Logf("Wrote batch of %d records to topic %s", i, topic3)
			time.Sleep(time.Millisecond * 100)
		}

		time.Sleep(1 * time.Second) // give it some time to consume the records

		// stop consuming & trigger the flush
		stopApp(ctx, t, cancel, errCh)

		filesFound := listFilesOnBucket(ctx, t, s3Client, s3Prefix)
		require.Len(t, filesFound, 1)
		// we expect the file to have records, but it might not have consumed all
		require.LessOrEqual(t, filesFound["flush-on-stop/flush-stop-1/0/flush-stop-1-0-0000000000000000000.avro"], 10000)
	})
}

func stopApp(ctx context.Context, t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()
	cancel()
	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		require.NoError(t, err, "backup returned unexpected error")
	case <-time.After(10 * time.Second):
		t.Fatal("backup did not finish after cancellation")
	}
}

func waitConsumerStart(ctx context.Context, t *testing.T, client *kadm.Client, groupId string) {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			t.Fatalf("consumer group %s did not start in 10 seconds", groupId)
		case <-time.Tick(100 * time.Millisecond):
			dg, err := client.DescribeGroups(ctx, groupId)
			require.NoError(t, err)
			if dg[groupId].State == "Stable" {
				t.Logf("consumer group %s started consuming", groupId)
				return
			}
		}
	}
}

func newRandomName(baseName string) string {
	return baseName + "-" + uuid.NewString()
}

func listFilesOnBucket(ctx context.Context, t *testing.T, s3Client *s3.Client, s3prefix string) map[string]int {
	t.Helper()
	filesFound := make(map[string]int)
	// List files in S3
	listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName), Prefix: aws.String(s3prefix)})
	require.NoError(t, err)
	require.NotEmpty(t, listResp.Contents, "expected files in S3 bucket")
	require.NoError(t, err)
	require.NotEmpty(t, listResp.Contents, "expected files in S3 bucket")

	t.Logf("Found %d files in S3", len(listResp.Contents))

	for _, obj := range listResp.Contents {
		key := *obj.Key

		getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		records := decodeAvroFile(t, getResp.Body)
		filesFound[key] = len(records)
		t.Logf("Found file: %s (size: %d), recs: %d", key, obj.Size, len(records))
	}
	return filesFound
}

func startKafkaService(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()
	redpandaContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v25.1.1")
	require.NoError(t, err)
	terminateFunc := func() {
		if err := redpandaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Redpanda container: %v", err)
		}
	}

	kafkaBrokers, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return kafkaBrokers, terminateFunc
}

func startS3Service(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()
	minioContainer, err := minio.Run(ctx, "minio/minio:latest", minio.WithUsername(s3User), minio.WithPassword(s3pass))
	require.NoError(t, err)

	terminateFunc := func() {
		if err := minioContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MinIO container: %v", err)
		}
	}

	// Get connection details for MinIO
	connString, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)
	return fmt.Sprintf("http://%s", connString), terminateFunc
}

func newS3Client(t *testing.T, ctx context.Context, s3Endpoint string) *s3.Client {
	t.Helper()
	awsCfg, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err)

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})
}

func setupEnvS3Access() {
	_ = os.Setenv("AWS_ACCESS_KEY_ID", s3User)
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", s3pass)
	_ = os.Setenv("AWS_REGION", minioRegion)
}

// Helper to wait for consumer group offsets
func waitForGroupOffsets(t *testing.T, ctx context.Context, client *kadm.Client, group string, expected map[string]int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			t.Fatalf("consumer group %s did not reach expected offsets: %+v", group, expected)
			return
		case <-time.Tick(100 * time.Millisecond):
			if isGroupAt(t, ctx, client, group, expected) {
				return
			}
		}
	}
}

func isGroupAt(t *testing.T, ctx context.Context, client *kadm.Client, group string, expected map[string]int) bool {
	topics := make([]string, 0, len(expected))
	for t := range expected {
		topics = append(topics, t)
	}

	offsets, err := client.FetchOffsetsForTopics(ctx, group, topics...)
	require.NoError(t, err)

	for topic, exp := range expected {
		topicOffsets, hasTopicOffset := offsets[topic]
		if !hasTopicOffset {
			t.Logf("Topic %s: no offsets found", topic)
			return false
		}

		currentOffset := getOffsetForTopic(topicOffsets)
		t.Logf("Topic %s: current offset sum: %d, expected: %d", topic, currentOffset, exp)
		if currentOffset < exp {
			return false
		}
	}

	return true
}

func getOffsetForTopic(topicOffsets map[int32]kadm.OffsetResponse) int {
	// Sum offsets for all partitions of the topic
	currentOffsetSum := 0
	for _, pOff := range topicOffsets {
		if pOff.At > 0 {
			currentOffsetSum += int(pOff.At)
		}
	}
	return currentOffsetSum
}

func writeRecords(t *testing.T, ctx context.Context, client *kgo.Client, topic string, partition int32, count int, totalBytes int64) {
	recs := make([]*kgo.Record, 0, count)
	for i := range count {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       fmt.Appendf(nil, "key-%s-p%d-%d", topic, partition, i),
			Value:     genBytes(t, totalBytes/int64(count)),
			Headers: []kgo.RecordHeader{
				{Key: "test-header", Value: fmt.Appendf(nil, "header-value-%d", i)},
			},
		}
		recs = append(recs, rec)
	}
	require.NoError(t, client.ProduceSync(ctx, recs...).FirstErr(), "failed to produce records")
}

func genBytes(t *testing.T, size int64) []byte {
	data := make([]byte, size)
	// This fills the slice with high-entropy random bits that should not be very compressable
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func decodeAvroFile(t *testing.T, r io.ReadCloser) []*kgo.Record {
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Failed to close Avro file reader: %v", err)
		}
	}()
	decFactory := &avro.RecordDecoderFactory{}
	decoder, err := decFactory.New(r)
	require.NoError(t, err)

	records := make([]*kgo.Record, 0, 1000)
	for decoder.HasNext() {
		rec, err := decoder.Decode()
		if err != nil {
			return nil
		}
		records = append(records, rec)
	}

	return records
}
