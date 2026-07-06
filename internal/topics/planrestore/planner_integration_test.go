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
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := t.Context()

	// 1. Start Services
	kafkaBrokers, tkf := testutil.StartKafkaService(t)
	defer tkf()

	s3Endpoint, ts3f := testutil.StartS3Service(t)
	defer ts3f()
	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(t, s3Endpoint)

	bucketName := "test-restore-bucket"
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	planTopic := "restore-plan"
	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
	)
	require.NoError(t, err)
	defer adminClient.Close()

	kadmClient := kadm.NewClient(adminClient)
	defer kadmClient.Close()
	// using a single partition for the plan topic to ensure ordering of records
	_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
	require.NoError(t, err)

	// Format: s3Prefix/topic/partition/filename
	createS3Objects(t, bucketName, []s3Object{
		{key: "kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro"},
		{key: "kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro"},
		{key: "kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro"},
		{key: "kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro"},
		{key: "kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro"},
		{key: "kafka-backup/not-included-topic/0/not-included-topic-0-0000000000000000050.avro"},
		{key: "kafka-backup/topic-excluded/0/topic-excluded-0-0000000000000000000.avro"},
	}, s3Client)

	// 3. Run planner
	cfg := AppConfig{
		KafkaConfig: kafka.Config{
			Brokers: kafkaBrokers,
		},
		PlanTopic: planTopic,
		S3: ints3.Config{
			Bucket:   bucketName,
			Endpoint: s3Endpoint,
			Region:   testutil.MinioRegion,
		},
		S3Prefix: "kafka-backup",

		RestoreTopicsRegex: "topic-.*",
		ExcludeTopicsRegex: "topic-excluded",
	}

	err = Run(ctx, cfg)
	require.NoError(t, err)

	// file-index is 1-based absolute, total-files is the partition total.
	// S3 lists objects lexicographically within each topic prefix; the single-partition plan
	// topic preserves production order end-to-end.
	expected := []planRecord{
		{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "2"},
		{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro", fileIndex: "2", totalFiles: "2"},
		{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "2"},
		{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro", fileIndex: "2", totalFiles: "2"},
		{value: "kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro", fileIndex: "1", totalFiles: "1"},
	}

	records, err := testutil.WaitForRecords(t, planTopic, kafkaBrokers, len(expected))
	require.NoError(t, err)

	checkPlannedEntries(t, expected, records)

	// write more entries in S3, to simulate a resume
	createS3Objects(t, bucketName, []s3Object{
		{key: "kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro"},
		{key: "kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro"},
		{key: "kafka-backup/topic-c/0/topic-c-0-0000000000000000000.avro"},
		{key: "kafka-backup/topic-c/1/topic-c-1-0000000000000000000.avro"},
	}, s3Client)

	// run it a second time -> it should read what's already in the topic and resume from there
	err = Run(ctx, cfg)
	require.NoError(t, err)

	// Run 1 records are re-read from offset 0; run 2 appends resumed records after.
	// topic-b/0's run-1 record carries total=1; the resumed record carries absolute index 2 and updated total 2.
	expectedAfterResume := []planRecord{
		{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "2"},
		{value: "kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro", fileIndex: "2", totalFiles: "2"},
		{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "2"},
		{value: "kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro", fileIndex: "2", totalFiles: "2"},
		{value: "kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro", fileIndex: "1", totalFiles: "1"},
		{value: "kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro", fileIndex: "2", totalFiles: "2"},
		{value: "kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "1"},
		{value: "kafka-backup/topic-c/0/topic-c-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "1"},
		{value: "kafka-backup/topic-c/1/topic-c-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "1"},
	}

	records, err = testutil.WaitForRecords(t, planTopic, kafkaBrokers, len(expectedAfterResume))
	require.NoError(t, err)

	checkPlannedEntries(t, expectedAfterResume, records)
}

// TestPlanRestoreLargeTopicsLast verifies that topics exceeding the threshold are deferred.
func TestPlanRestoreLargeTopicsLast(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := t.Context()

	kafkaBrokers, tkf := testutil.StartKafkaService(t)
	defer tkf()

	s3Endpoint, ts3f := testutil.StartS3Service(t)
	defer ts3f()
	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(t, s3Endpoint)

	bucketName := "test-large-last-bucket"
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	planTopic := "restore-plan-large"
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers))
	require.NoError(t, err)
	defer adminClient.Close()

	kadmClient := kadm.NewClient(adminClient)
	defer kadmClient.Close()
	// using a single partition for the plan topic to ensure ordering of records
	_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
	require.NoError(t, err)

	// small-topic: 2 partitions, tiny files. large-topic: 2 partitions, multiple 600 KB files
	// each → well above the 1 MB threshold, triggering deferral.
	const largeFileBytes = 600 * 1024
	createS3Objects(t, bucketName, []s3Object{
		{key: "prefix/small-topic/0/small-topic-0-0000000000000000000.avro", sizeBytes: 5},
		{key: "prefix/small-topic/0/small-topic-0-0000000000000000100.avro", sizeBytes: 5},
		{key: "prefix/small-topic/1/small-topic-1-0000000000000000000.avro", sizeBytes: 5},
		{key: "prefix/large-topic/0/large-topic-0-0000000000000000000.avro", sizeBytes: largeFileBytes},
		{key: "prefix/large-topic/0/large-topic-0-0000000000000000500.avro", sizeBytes: largeFileBytes},
		{key: "prefix/large-topic/0/large-topic-0-0000000000000001000.avro", sizeBytes: largeFileBytes},
		{key: "prefix/large-topic/1/large-topic-1-0000000000000000000.avro", sizeBytes: largeFileBytes},
		{key: "prefix/large-topic/1/large-topic-1-0000000000000000200.avro", sizeBytes: largeFileBytes},
	}, s3Client)

	// Set threshold to 1 MB so large-topic (3 MB total) is deferred.
	cfg := AppConfig{
		KafkaConfig: kafka.Config{Brokers: kafkaBrokers},
		PlanTopic:   planTopic,
		S3: ints3.Config{
			Bucket:   bucketName,
			Endpoint: s3Endpoint,
			Region:   testutil.MinioRegion,
		},
		S3Prefix:               "prefix",
		RestoreTopicsRegex:     ".*",
		ProcessLargeTopicsLast: true,
		LargeTopicThresholdMB:  1,
	}

	err = Run(ctx, cfg)
	require.NoError(t, err)

	// listTopicsFromS3 discovers large-topic before small-topic (lexicographic order), but
	// reorderLargeTopicsLast moves it to the end. The expected slice encodes the resulting
	// production order: all small-topic records first, then large-topic records.
	expected := []planRecord{
		{value: "prefix/small-topic/0/small-topic-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "2"},
		{value: "prefix/small-topic/0/small-topic-0-0000000000000000100.avro", fileIndex: "2", totalFiles: "2"},
		{value: "prefix/small-topic/1/small-topic-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "1"},
		{value: "prefix/large-topic/0/large-topic-0-0000000000000000000.avro", fileIndex: "1", totalFiles: "3"},
		{value: "prefix/large-topic/0/large-topic-0-0000000000000000500.avro", fileIndex: "2", totalFiles: "3"},
		{value: "prefix/large-topic/0/large-topic-0-0000000000000001000.avro", fileIndex: "3", totalFiles: "3"},
		{value: "prefix/large-topic/1/large-topic-1-0000000000000000000.avro", fileIndex: "1", totalFiles: "2"},
		{value: "prefix/large-topic/1/large-topic-1-0000000000000000200.avro", fileIndex: "2", totalFiles: "2"},
	}

	records, err := testutil.WaitForRecords(t, planTopic, kafkaBrokers, len(expected))
	require.NoError(t, err)

	checkPlannedEntries(t, expected, records)
}

type planRecord struct {
	value      string
	fileIndex  string
	totalFiles string
}

// checkPlannedEntries asserts that records matches expected exactly, in order.
// Because the plan topic is a single partition, the production order is preserved end-to-end.
func checkPlannedEntries(t *testing.T, expected []planRecord, records []*kgo.Record) {
	t.Helper()
	require.Len(t, records, len(expected), "wrong number of records")
	for i, want := range expected {
		rec := records[i]
		assert.Equal(t, want.value, string(rec.Value), "record %d: wrong value", i)
		gotIdx, hasIdx := getHeader(rec, FileIndexHeader)
		gotTotal, hasTotal := getHeader(rec, TotalFilesHeader)
		assert.True(t, hasIdx, "record %d missing %s", i, FileIndexHeader)
		assert.True(t, hasTotal, "record %d missing %s", i, TotalFilesHeader)
		assert.Equal(t, want.fileIndex, gotIdx, "record %d: wrong file-index", i)
		assert.Equal(t, want.totalFiles, gotTotal, "record %d: wrong total-files", i)
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
