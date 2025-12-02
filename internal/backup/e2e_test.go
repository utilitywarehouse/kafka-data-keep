package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	"os"
)

func TestE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx := context.Background()

	// Start MinIO container (S3-compatible storage)
	minioContainer, err := minio.Run(ctx, "minio/minio:latest")//minio.WithUsername("uwminio"),
	//minio.WithPassword("minioadmin"),

	require.NoError(t, err)
	defer func() {
		require.NoError(t, minioContainer.Terminate(ctx))
	}()

	// Start Redpanda container
	redpandaContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v25.1.1")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, redpandaContainer.Terminate(context.Background()))
	}()

	// Get connection details for MinIO
	connString, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)
	s3Endpoint := fmt.Sprintf("http://%s", connString)

	kafkaBrokers, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	// Setup S3 client for test setup and verification
	// Also set env credentials for the app under test (Run uses default AWS config chain)
	_ = os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		// MinIO default credentials
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	require.NoError(t, err)

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})

	// Create S3 bucket
	bucketName := "test-backup-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Create Kafka topics
	topic1 := "test-topic-1"
	topic2 := "test-topic-2"

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers))
	require.NoError(t, err)
	defer adminClient.Close()

	kadmClient := kadm.NewClient(adminClient)
	defer kadmClient.Close()

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
		Bucket:      bucketName,
		FileSize:    1, // Small file size to force frequent flushes
		WorkingDir:  workingDir,
		S3Endpoint:  s3Endpoint,
		S3Region:    "us-east-1",
	}

	// Create kadm client for offset checking
	admClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers))
	require.NoError(t, err)
	defer admClient.Close()

	// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
	backupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Run backup in a goroutine using the main app's Run
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(backupCtx, cfg)
	}()

	// Write test records to Kafka
	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // set the partitions manually on produce
	)
	require.NoError(t, err)
	defer producerClient.Close()

	// First batch of records
	writeRecords(t, ctx, producerClient, topic1, 0, 10)
	writeRecords(t, ctx, producerClient, topic1, 1, 20)
	writeRecords(t, ctx, producerClient, topic2, 0, 20)
	writeRecords(t, ctx, producerClient, topic2, 1, 30)

	// Wait until these records are consumed
	waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 30, topic2: 50})

	// Second batch of records
	writeRecords(t, ctx, producerClient, topic1, 0, 20)
	writeRecords(t, ctx, producerClient, topic1, 1, 10)
	writeRecords(t, ctx, producerClient, topic2, 0, 30)
	writeRecords(t, ctx, producerClient, topic2, 1, 40)
	// Wait until second batch is consumed
	waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 60, topic2: 120})

	// Flush to ensure all records are sent
	require.NoError(t, producerClient.Flush(ctx))

	// Wait for backup to finish after cancellation
	cancel() // cancel the backup context after second batch is consumed
	select {
	case err := <-errCh:
		if err != nil &&
			!errors.Is(err, context.Canceled) &&
			!strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("backup returned unexpected error: %v", err)
		}
		// Expected graceful shutdown
	case <-time.After(10 * time.Second):
		t.Fatal("backup did not finish after cancellation")
	}

	// Wait a bit for S3 uploads to settle
	time.Sleep(2 * time.Second)

	// List files in S3
	listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listResp.Contents, "expected files in S3 bucket")

	t.Logf("Found %d files in S3", len(listResp.Contents))

	// Verify files
	expectedTopics := map[string]bool{topic1: true, topic2: true}
	topicsFound := make(map[string]bool)

	for _, obj := range listResp.Contents {
		key := *obj.Key
		t.Logf("Found file: %s (size: %d)", key, obj.Size)

		// Download and verify file
		getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		// Read file content
		content, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		require.NotEmpty(t, content, "file should not be empty")

		// Decode Avro records
		records, err := decodeAvroFile(content)
		require.NoError(t, err)
		require.NotEmpty(t, records, "file should contain records")

		// Verify records belong to expected topics
		for _, rec := range records {
			if expectedTopics[rec.Topic] {
				topicsFound[rec.Topic] = true
			}
		}

		t.Logf("  Contains %d records", len(records))
	}

	// Verify we have files for all expected topics
	for topic := range expectedTopics {
		require.True(t, topicsFound[topic], "expected to find files for topic %s", topic)
	}
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

func decodeAvroFile(content []byte) ([]*kgo.Record, error) {
	decFactory := &avro.RecordDecoderFactory{}
	reader := io.NopCloser(bytes.NewReader(content))
	decoder, err := decFactory.New(reader)
	if err != nil {
		return nil, err
	}

	var records []*kgo.Record
	for decoder.HasNext() {
		rec, err := decoder.Decode()
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}
