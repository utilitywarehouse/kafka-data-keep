package backup

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	"os"
)

const minioRegion = "us-east-1"
const bucketName = "test-backup-bucket"
const s3User = "uwadmin"
const s3pass = "uwadminpass"

func TestBackupIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda container
	kafkaBrokers, tkf := startKafkaService(t, ctx)
	defer tkf()

	s3Endpoint, ts3f := startS3Service(t, ctx)
	defer ts3f()

	setupEnvS3Access()
	s3Client := newS3Client(t, ctx, s3Endpoint)

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // set the partitions manually on produce
	)
	require.NoError(t, err)
	defer adminClient.Close()

	kadmClient := kadm.NewClient(adminClient)
	defer kadmClient.Close()

	// Create Kafka topics
	topic1 := "test-topic-1"
	topic2 := "test-topic-2"
	_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic1)
	require.NoError(t, err)

	_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic2)
	require.NoError(t, err)

	// Setup backup application config
	workingDir := t.TempDir()

	const groupID = "test-backup-group"
	cfg := AppConfig{
		Brokers:     kafkaBrokers,
		TopicsRegex: ".*",
		GroupID:     groupID,
		MinFileSize: 400, // This is the current min file size in this test, to obtain a single file per batch written
		WorkingDir:  workingDir,
		S3Bucket:    bucketName,
		S3Prefix:    "integ-test/",
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

	// First batch of records
	writeRecords(t, ctx, adminClient, topic1, 0, 10)
	writeRecords(t, ctx, adminClient, topic1, 1, 20)
	writeRecords(t, ctx, adminClient, topic2, 0, 20)
	writeRecords(t, ctx, adminClient, topic2, 1, 30)
	require.NoError(t, adminClient.Flush(ctx))

	// Wait until these records are consumed
	waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 30, topic2: 50})

	// Second batch of records
	writeRecords(t, ctx, adminClient, topic1, 0, 20)
	writeRecords(t, ctx, adminClient, topic1, 1, 10)
	writeRecords(t, ctx, adminClient, topic2, 0, 30)
	writeRecords(t, ctx, adminClient, topic2, 1, 40)
	require.NoError(t, adminClient.Flush(ctx))

	// Wait until the second batch is consumed
	waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 60, topic2: 120})

	cancel() // cancel the backup context after the second batch is consumed
	// Wait for backup to finish after cancellation
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("backup returned unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("backup did not finish after cancellation")
	}

	expectedFiles := map[string]int{
		"integ-test/test-topic-1/0/test-topic-1-0-0000000000000000000.avro": 10,
		"integ-test/test-topic-1/0/test-topic-1-0-0000000000000000010.avro": 20,
		"integ-test/test-topic-1/1/test-topic-1-1-0000000000000000000.avro": 20,
		"integ-test/test-topic-1/1/test-topic-1-1-0000000000000000020.avro": 10,
		"integ-test/test-topic-2/0/test-topic-2-0-0000000000000000000.avro": 20,
		"integ-test/test-topic-2/0/test-topic-2-0-0000000000000000020.avro": 30,
		"integ-test/test-topic-2/1/test-topic-2-1-0000000000000000000.avro": 30,
		"integ-test/test-topic-2/1/test-topic-2-1-0000000000000000030.avro": 40,
	}

	filesFound := listFilesOnBucket(ctx, t, err, s3Client)

	require.Equal(t, expectedFiles, filesFound)
}

func listFilesOnBucket(ctx context.Context, t *testing.T, err error, s3Client *s3.Client) map[string]int {
	filesFound := make(map[string]int)
	// List files in S3
	listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	require.NoError(t, err)
	require.NotEmpty(t, listResp.Contents, "expected files in S3 bucket")

	t.Logf("Found %d files in S3", len(listResp.Contents))

	for _, obj := range listResp.Contents {
		key := *obj.Key
		t.Logf("Found file: %s (size: %d)", key, obj.Size)

		getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		records := decodeAvroFile(t, getResp.Body)
		filesFound[key] = len(records)
	}
	return filesFound
}

func startKafkaService(t *testing.T, ctx context.Context) (string, func()) {
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
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFunc()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("consumer group %s did not reach expected offsets: %+v", group, expected)
			return
		case <-ticker.C:
			if isGroupAt(t, timeoutCtx, client, group, expected) {
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

func writeRecords(t *testing.T, ctx context.Context, client *kgo.Client, topic string, partition int32, count int) {
	var recs []*kgo.Record
	for i := 0; i < count; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       []byte(fmt.Sprintf("key-%s-p%d-%d", topic, partition, i)),
			Value:     []byte(fmt.Sprintf("value-%s-p%d-%d", topic, partition, i)),
			Headers: []kgo.RecordHeader{
				{Key: "test-header", Value: []byte(fmt.Sprintf("header-value-%d", i))},
			},
		}
		recs = append(recs, rec)
	}
	require.NoError(t, client.ProduceSync(ctx, recs...).FirstErr(), "failed to produce records")
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

	var records []*kgo.Record
	for decoder.HasNext() {
		rec, err := decoder.Decode()
		if err != nil {
			return nil
		}
		records = append(records, rec)
	}

	return records
}
