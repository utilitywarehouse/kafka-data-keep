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
	_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
	require.NoError(t, err)

	// Format: s3Prefix/topic/partition/filename
	files := []string{
		"kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro",
		"kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro",
		"kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro",
		"kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro",
		"kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro",
		"kafka-backup/not-included-topic/0/not-included-topic-0-0000000000000000050.avro",
		"kafka-backup/topic-excluded/0/topic-excluded-0-0000000000000000000.avro",
	}

	createS3Objects(t, bucketName, files, s3Client)

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

	// map[messageKey] -> list of messageValues (full s3 keys)
	expectedMap := map[string][]string{
		"kafka-backup/topic-a/0/": {
			"kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro",
			"kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro",
		},
		"kafka-backup/topic-a/1/": {
			"kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro",
			"kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro",
		},
		"kafka-backup/topic-b/0/": {
			"kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro",
		},
	}

	records, err := testutil.WaitForRecords(t, planTopic, kafkaBrokers, totalMapValues(expectedMap))
	require.NoError(t, err)

	checkPlannedEntries(t, expectedMap, records)

	// Expected headers: file-index is 1-based absolute, total-files is partition total.
	// topic-a/0 has 2 files, topic-a/1 has 2 files, topic-b/0 has 1 file.
	expectedHeaders := map[string][]planRecordHeaders{
		"kafka-backup/topic-a/0/": {
			{fileIndex: "1", totalFiles: "2"},
			{fileIndex: "2", totalFiles: "2"},
		},
		"kafka-backup/topic-a/1/": {
			{fileIndex: "1", totalFiles: "2"},
			{fileIndex: "2", totalFiles: "2"},
		},
		"kafka-backup/topic-b/0/": {
			{fileIndex: "1", totalFiles: "1"},
		},
	}
	checkPlanHeaders(t, expectedHeaders, records)

	//	write more entries in S3, to simulate a resume
	files = []string{
		"kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro",
		"kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro",
		"kafka-backup/topic-c/0/topic-c-0-0000000000000000000.avro",
		"kafka-backup/topic-c/1/topic-c-1-0000000000000000000.avro",
	}

	createS3Objects(t, bucketName, files, s3Client)

	// run it a second time -> it should read what's already in the topic and resume from there
	err = Run(ctx, cfg)
	require.NoError(t, err)

	expectedMap = map[string][]string{
		"kafka-backup/topic-a/0/": {
			"kafka-backup/topic-a/0/topic-a-0-0000000000000000000.avro",
			"kafka-backup/topic-a/0/topic-a-0-0000000000000000010.avro",
		},
		"kafka-backup/topic-a/1/": {
			"kafka-backup/topic-a/1/topic-a-1-0000000000000000000.avro",
			"kafka-backup/topic-a/1/topic-a-1-0000000000000000300.avro",
		},
		"kafka-backup/topic-b/0/": {
			"kafka-backup/topic-b/0/topic-b-0-0000000000000000050.avro",
			"kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro",
		},
		"kafka-backup/topic-b/1/": {
			"kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro",
		},
		"kafka-backup/topic-c/0/": {
			"kafka-backup/topic-c/0/topic-c-0-0000000000000000000.avro",
		},
		"kafka-backup/topic-c/1/": {
			"kafka-backup/topic-c/1/topic-c-1-0000000000000000000.avro",
		},
	}

	records, err = testutil.WaitForRecords(t, planTopic, kafkaBrokers, totalMapValues(expectedMap))
	require.NoError(t, err)

	checkPlannedEntries(t, expectedMap, records)

	// On resume, topic-b/0 now has 2 total files. The resumed file (index 2) must show the
	// absolute index, confirming the planner counts from the start even on resume.
	expectedHeadersAfterResume := map[string][]planRecordHeaders{
		"kafka-backup/topic-b/0/": {
			{fileIndex: "1", totalFiles: "2"},
			{fileIndex: "2", totalFiles: "2"},
		},
		"kafka-backup/topic-b/1/": {
			{fileIndex: "1", totalFiles: "1"},
		},
		"kafka-backup/topic-c/0/": {
			{fileIndex: "1", totalFiles: "1"},
		},
		"kafka-backup/topic-c/1/": {
			{fileIndex: "1", totalFiles: "1"},
		},
	}
	checkPlanHeaders(t, expectedHeadersAfterResume, records)
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
	_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, planTopic)
	require.NoError(t, err)

	// small-topic: 1 file (small body), large-topic: 1 file (large body ~2 MB).
	smallBody := strings.NewReader("small")
	largeBody := strings.NewReader(strings.Repeat("x", 2*1024*1024))

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("prefix/small-topic/0/small-topic-0-0000000000000000000.avro"),
		Body:   smallBody,
	})
	require.NoError(t, err)

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("prefix/large-topic/0/large-topic-0-0000000000000000000.avro"),
		Body:   largeBody,
	})
	require.NoError(t, err)

	// Set threshold to 1 MB so large-topic (2 MB) is deferred.
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

	records, err := testutil.WaitForRecords(t, planTopic, kafkaBrokers, 2)
	require.NoError(t, err)

	// Collect values in order.
	values := make([]string, len(records))
	for i, rec := range records {
		values[i] = string(rec.Value)
	}

	// small-topic must appear before large-topic.
	smallIdx := -1
	largeIdx := -1
	for i, v := range values {
		if strings.Contains(v, "small-topic") {
			smallIdx = i
		}
		if strings.Contains(v, "large-topic") {
			largeIdx = i
		}
	}
	require.NotEqual(t, -1, smallIdx, "small-topic file not found in plan records")
	require.NotEqual(t, -1, largeIdx, "large-topic file not found in plan records")
	assert.Less(t, smallIdx, largeIdx, "small-topic should appear before large-topic in the plan")
}

type planRecordHeaders struct {
	fileIndex  string
	totalFiles string
}

// checkPlanHeaders asserts that each record with a matching partition key carries the
// expected plan-restore.file-index and plan-restore.total-files headers, in order.
func checkPlanHeaders(t *testing.T, expected map[string][]planRecordHeaders, records []*kgo.Record) {
	t.Helper()
	// Collect records per partition key, in receive order.
	byKey := make(map[string][]*kgo.Record)
	for _, rec := range records {
		k := string(rec.Key)
		byKey[k] = append(byKey[k], rec)
	}

	for partKey, wantHeaders := range expected {
		recs, ok := byKey[partKey]
		if !ok {
			// Partition key may span multiple plan runs; only assert when present.
			continue
		}
		require.GreaterOrEqual(t, len(recs), len(wantHeaders),
			"partition %s: not enough records to check headers", partKey)

		// Match by position within this partition's records.
		for i, want := range wantHeaders {
			rec := recs[i]
			gotIdx, hasIdx := getHeader(rec, FileIndexHeader)
			gotTotal, hasTotal := getHeader(rec, TotalFilesHeader)
			assert.True(t, hasIdx, "record %d for %s missing %s", i, partKey, FileIndexHeader)
			assert.True(t, hasTotal, "record %d for %s missing %s", i, partKey, TotalFilesHeader)
			assert.Equal(t, want.fileIndex, gotIdx, "record %d for %s: wrong file-index", i, partKey)
			assert.Equal(t, want.totalFiles, gotTotal, "record %d for %s: wrong total-files", i, partKey)
		}
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

func totalMapValues(expectedMap map[string][]string) int {
	totalExpected := 0
	for _, v := range expectedMap {
		totalExpected += len(v)
	}
	return totalExpected
}

func checkPlannedEntries(t *testing.T, expectedMap map[string][]string, records []*kgo.Record) {
	t.Helper()
	consumedMap := make(map[string][]string)
	for _, record := range records {
		key := string(record.Key)
		val := string(record.Value)
		consumedMap[key] = append(consumedMap[key], val)
	}

	require.Len(t, consumedMap, len(expectedMap))
	for k, expectedVals := range expectedMap {
		require.Equal(t, expectedVals, consumedMap[k], "mismatch for key %s", k)
	}
}

func createS3Objects(t *testing.T, bucketName string, files []string, s3Client *s3.Client) {
	t.Helper()
	for _, f := range files {
		_, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f),
			Body:   strings.NewReader("dummy content"),
		})
		require.NoError(t, err)
	}
}
