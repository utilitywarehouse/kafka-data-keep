package planrestore

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
)

func TestPlanRestoreIntegration(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaBrokers, tkf := testutil.StartKafkaService(t)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := testutil.StartS3Service(t)
	t.Cleanup(ts3f)
	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(t, s3Endpoint)

	const bucketName = "test-planrestore-bucket"
	_, err := s3Client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers))
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)

	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	t.Run("resume", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		// Single-partition plan topic to preserve production order end-to-end.
		planTopic := "restore-plan-resume"
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
		require.NoError(t, err)

		createS3Objects(t, bucketName, []s3Object{
			{key: "kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro"},
			{key: "kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro"},
			{key: "kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro"},
			{key: "kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro"},
			{key: "kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro"},
			{key: "kafka-backup/not-included-topic/0/not-included-topic-0-0000000000000000050.avro"},
			{key: "kafka-backup/topic-excluded/0/topic-excluded-0-0000000000000000000.avro"},
		}, s3Client)

		cfg := AppConfig{
			KafkaConfig:        kafka.Config{Brokers: kafkaBrokers},
			PlanTopic:          planTopic,
			S3:                 ints3.Config{Bucket: bucketName, Endpoint: s3Endpoint, Region: testutil.MinioRegion},
			S3Prefix:           "kafka-backup",
			RestoreTopicsRegex: "topic-.*",
			ExcludeTopicsRegex: "topic-excluded",
		}

		err = Run(ctx, cfg)
		require.NoError(t, err)

		// file-index is 1-based absolute; S3 lists lexicographically within each topic prefix.
		// topic-a: 2 files in partition 0 + 2 in partition 1 = 4 topic-total-files.
		// topic-b: 1 file in partition 0 (partition 1 doesn't exist yet) = 1 topic-total-files.
		expected := []planRecord{
			{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro", fileIndex: "1", totalFiles: "1", topicTotalFiles: "1"},
		}

		records, err := testutil.WaitForRecords(t, planTopic, kafkaBrokers, len(expected))
		require.NoError(t, err)
		checkPlannedEntries(t, expected, records)

		// Add more S3 objects to simulate a resume on the next run.
		createS3Objects(t, bucketName, []s3Object{
			{key: "kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro"},
			{key: "kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro"},
		}, s3Client)

		err = Run(ctx, cfg)
		require.NoError(t, err)

		// Run-1 records are re-read from offset 0; run-2 appends resumed records after.
		// topic-b/0's run-1 record carries total=1 and topic-total=1 (partition 1 didn't exist
		// yet); the resumed records reflect the updated partition/topic totals (3 files total:
		// 2 in partition 0, 1 in partition 1).
		expectedAfterResume := []planRecord{
			{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "4"},
			{value: "kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro", fileIndex: "1", totalFiles: "1", topicTotalFiles: "1"},
			{value: "kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "3"},
			{value: "kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "1", topicTotalFiles: "3"},
		}
		records, err = testutil.WaitForRecords(t, planTopic, kafkaBrokers, len(expectedAfterResume))
		require.NoError(t, err)
		checkPlannedEntries(t, expectedAfterResume, records)
	})

	t.Run("large topics last", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		// Single-partition plan topic to preserve production order end-to-end.
		planTopic := "restore-plan-large-last"
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
		require.NoError(t, err)

		// small-topic: 2 partitions, tiny files. large-topic: 2 partitions, multiple 600 KB files
		// each → well above the 1 MB threshold, triggering deferral.
		const largeFileBytes = 600 * 1024
		createS3Objects(t, bucketName, []s3Object{
			{key: "large-last/small-topic/0/small-topic-0-0000000000000000000.avro", sizeBytes: 5},
			{key: "large-last/small-topic/0/small-topic-0-0000000000000000100.avro", sizeBytes: 5},
			{key: "large-last/small-topic/1/small-topic-1-0000000000000000000.avro", sizeBytes: 5},
			{key: "large-last/large-topic/0/large-topic-0-0000000000000000000.avro", sizeBytes: largeFileBytes},
			{key: "large-last/large-topic/0/large-topic-0-0000000000000000500.avro", sizeBytes: largeFileBytes},
			{key: "large-last/large-topic/0/large-topic-0-0000000000000001000.avro", sizeBytes: largeFileBytes},
			{key: "large-last/large-topic/1/large-topic-1-0000000000000000000.avro", sizeBytes: largeFileBytes},
			{key: "large-last/large-topic/1/large-topic-1-0000000000000000200.avro", sizeBytes: largeFileBytes},
		}, s3Client)

		// Set threshold to 1 MB so large-topic (~3 MB total) is deferred.
		cfg := AppConfig{
			KafkaConfig:            kafka.Config{Brokers: kafkaBrokers},
			PlanTopic:              planTopic,
			S3:                     ints3.Config{Bucket: bucketName, Endpoint: s3Endpoint, Region: testutil.MinioRegion},
			S3Prefix:               "large-last",
			RestoreTopicsRegex:     ".*",
			ProcessLargeTopicsLast: true,
			LargeTopicThresholdMB:  1,
		}

		err = Run(ctx, cfg)
		require.NoError(t, err)

		// listTopicsFromS3 discovers large-topic before small-topic (lexicographic order), but
		// reorderLargeTopicsLast moves it to the end. The slice encodes the resulting production
		// order: all small-topic records first, then large-topic records.
		// small-topic: 2 files in partition 0 + 1 in partition 1 = 3 topic-total-files.
		// large-topic: 3 files in partition 0 + 2 in partition 1 = 5 topic-total-files.
		expected := []planRecord{
			{value: "large-last/small-topic/0/small-topic-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "2", topicTotalFiles: "3"},
			{value: "large-last/small-topic/0/small-topic-0-0000000000000000100.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "3"},
			{value: "large-last/small-topic/1/small-topic-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "1", topicTotalFiles: "3"},
			{value: "large-last/large-topic/0/large-topic-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "3", topicTotalFiles: "5"},
			{value: "large-last/large-topic/0/large-topic-0-0000000000000000500.avro", fileIndex: "2", totalFiles: "3", topicTotalFiles: "5"},
			{value: "large-last/large-topic/0/large-topic-0-0000000000000001000.avro", fileIndex: "3", totalFiles: "3", topicTotalFiles: "5"},
			{value: "large-last/large-topic/1/large-topic-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "2", topicTotalFiles: "5"},
			{value: "large-last/large-topic/1/large-topic-1-0000000000000000200.avro", fileIndex: "2", totalFiles: "2", topicTotalFiles: "5"},
		}

		records, err := testutil.WaitForRecords(t, planTopic, kafkaBrokers, len(expected))
		require.NoError(t, err)
		checkPlannedEntries(t, expected, records)
	})

	t.Run("resume fails on topic mismatch", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		// Use a dedicated S3 prefix to avoid interference with other sub-tests.
		planTopic := "restore-plan-topic-mismatch"
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
		require.NoError(t, err)

		createS3Objects(t, bucketName, []s3Object{
			{key: "mismatch/topic-x/0/topic-x-0-0000000000000000001.avro"},
			{key: "mismatch/topic-y/0/topic-y-0-0000000000000000001.avro"},
		}, s3Client)

		// First run: only topic-x is in scope.
		cfgFirst := AppConfig{
			KafkaConfig:        kafka.Config{Brokers: kafkaBrokers},
			PlanTopic:          planTopic,
			S3:                 ints3.Config{Bucket: bucketName, Endpoint: s3Endpoint, Region: testutil.MinioRegion},
			S3Prefix:           "mismatch",
			RestoreTopicsRegex: "topic-x",
		}
		err = Run(ctx, cfgFirst)
		require.NoError(t, err)

		_, err = testutil.WaitForRecords(t, planTopic, kafkaBrokers, 1)
		require.NoError(t, err)

		// Second run: topic-y is added to scope — different topic set, SHA mismatch.
		cfgSecond := cfgFirst
		cfgSecond.RestoreTopicsRegex = "topic-x, topic-y"
		err = Run(ctx, cfgSecond)
		require.ErrorContains(t, err, "topics SHA mismatch")
	})
}

type planRecord struct {
	value           string
	fileIndex       string
	totalFiles      string
	topicTotalFiles string
}

// checkPlannedEntries asserts that records matches expected exactly, in order.
// Because the plan topic is a single partition, the production order is preserved end-to-end.
func checkPlannedEntries(t *testing.T, expected []planRecord, records []*kgo.Record) {
	t.Helper()
	require.Len(t, records, len(expected), "wrong number of records")
	for i, want := range expected {
		rec := records[i]
		assert.Equal(t, want.value, string(rec.Value), "record %d: wrong value", i)
		gotIdx, hasIdx := getHeader(rec, PartitionFileIndexHeader)
		gotTotal, hasTotal := getHeader(rec, PartitionTotalFilesHeader)
		gotTopicTotal, hasTopicTotal := getHeader(rec, TopicTotalFilesHeader)
		assert.True(t, hasIdx, "record %d missing %s", i, PartitionFileIndexHeader)
		assert.True(t, hasTotal, "record %d missing %s", i, PartitionTotalFilesHeader)
		assert.True(t, hasTopicTotal, "record %d missing %s", i, TopicTotalFilesHeader)
		assert.Equal(t, want.fileIndex, gotIdx, "record %d: wrong file-index", i)
		assert.Equal(t, want.totalFiles, gotTotal, "record %d: wrong total-files", i)
		assert.Equal(t, want.topicTotalFiles, gotTopicTotal, "record %d: wrong topic-total-files", i)
	}
}

func getHeader(rec *kgo.Record, key string) (string, bool) {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

type s3Object struct {
	key       string
	sizeBytes int
}

func createS3Objects(t *testing.T, bucketName string, objects []s3Object, s3Client *s3.Client) {
	t.Helper()
	for _, o := range objects {
		_, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(o.key),
			Body:   strings.NewReader(strings.Repeat("x", o.sizeBytes)),
		})
		require.NoError(t, err)
	}
}
