package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
)

func TestE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx := context.Background()

	// Start MinIO container (S3-compatible storage)
	minioContainer, err := minio.Run(ctx, "minio/minio:latest")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, minioContainer.Terminate(ctx))
	}()

	// Start Redpanda container
	redpandaContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v25.1.1")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, redpandaContainer.Terminate(ctx))
	}()

	// Get connection details for MinIO
	// Get the mapped host and port (MinIO API on 9000)
	host, err := minioContainer.Host(ctx)
	require.NoError(t, err)
	port, err := minioContainer.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	s3Endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	kafkaBrokers, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	// Give Redpanda a moment to stabilize before creating topics
	time.Sleep(2 * time.Second)

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

	createTopicsReq := kmsg.NewPtrCreateTopicsRequest()
	createTopicsReq.Topics = []kmsg.CreateTopicsRequestTopic{
		{
			Topic:             topic1,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		{
			Topic:             topic2,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}
	createTopicsResp, err := createTopicsReq.RequestWith(ctx, adminClient)
	require.NoError(t, err)
	for _, topicResp := range createTopicsResp.Topics {
		if topicResp.ErrorCode != 0 {
			t.Fatalf("failed to create topic %s: error code %d", topicResp.Topic, topicResp.ErrorCode)
		}
	}

	// Setup backup application config
	workingDir := t.TempDir()

	cfg := AppConfig{
		Brokers:     kafkaBrokers,
		TopicsRegex: ".*",
		GroupID:     "test-backup-group",
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
	kadmClient := kadm.NewClient(admClient)

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
	)
	require.NoError(t, err)
	defer producerClient.Close()

	// First batch of records
	writeRecords(t, ctx, producerClient, topic1, 0, 10)
	writeRecords(t, ctx, producerClient, topic1, 1, 10)
	writeRecords(t, ctx, producerClient, topic2, 0, 10)
	writeRecords(t, ctx, producerClient, topic2, 1, 10)

	// Wait until these records are consumed
	waitForGroupOffsets(t, ctx, kadmClient, "test-backup-group", map[string]int{topic1: 20, topic2: 20})

	// Second batch of records
	writeRecords(t, ctx, producerClient, topic1, 0, 10)
	writeRecords(t, ctx, producerClient, topic1, 1, 10)
	writeRecords(t, ctx, producerClient, topic2, 0, 10)
	writeRecords(t, ctx, producerClient, topic2, 1, 10)
	// Wait until second batch is consumed
	waitForGroupOffsets(t, ctx, kadmClient, "test-backup-group", map[string]int{topic1: 40, topic2: 40})

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
	deadline := time.Now().Add(30 * time.Second)

	topics := make([]string, 0, len(expected))
	for t := range expected {
		topics = append(topics, t)
	}

	for time.Now().Before(deadline) {
		offsets, err := client.FetchOffsetsForTopics(ctx, group, topics...)
		require.NoError(t, err)
		allMet := true
		for topic, exp := range expected {
			if topicOffsets, ok := offsets[topic]; ok {
				// Sum offsets for all partitions of the topic
				currentOffsetSum := 0
				for _, pOff := range topicOffsets {
					if pOff.At > 0 {
						currentOffsetSum += int(pOff.At)
					}
				}
				t.Logf("Topic %s: current offset sum: %d, expected: %d", topic, currentOffsetSum, exp)
				if currentOffsetSum < exp {
					allMet = false
					break
				}
			} else {
				t.Logf("Topic %s: no offsets found", topic)
				allMet = false
				break
			}
		}
		if allMet {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("consumer group %s did not reach expected offsets: %+v", group, expected)
}

func writeRecords(t *testing.T, ctx context.Context, client *kgo.Client, topic string, partition int32, count int) {
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
		client.Produce(ctx, rec, func(r *kgo.Record, err error) {
			require.NoError(t, err)
		})
	}
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
