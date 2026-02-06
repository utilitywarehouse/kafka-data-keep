package planrestore

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
)

func TestPlanRestoreIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// 1. Start Services
	kafkaBrokers, tkf := testutil.StartKafkaService(ctx, t)
	defer tkf()

	s3Endpoint, ts3f := testutil.StartS3Service(ctx, t)
	defer ts3f()
	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(ctx, t, s3Endpoint)

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

	createS3Objects(ctx, t, bucketName, files, s3Client)

	// 3. Run planner
	cfg := AppConfig{
		Brokers:            kafkaBrokers,
		PlanTopic:          planTopic,
		S3Bucket:           bucketName,
		S3Prefix:           "kafka-backup",
		S3Endpoint:         s3Endpoint,
		S3Region:           testutil.MinioRegion,
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

	records, err := testutil.WaitForRecords(ctx, t, planTopic, kafkaBrokers, totalMapValues(expectedMap))
	require.NoError(t, err)

	checkPlannedEntries(t, expectedMap, records)

	//	write more entries in S3, to simulate a resume
	files = []string{
		"kafka-backup/topic-b/0/topic-b-0-0000000000000000250.avro",
		"kafka-backup/topic-b/1/topic-b-1-0000000000000000000.avro",
		"kafka-backup/topic-c/0/topic-c-0-0000000000000000000.avro",
		"kafka-backup/topic-c/1/topic-c-1-0000000000000000000.avro",
	}

	createS3Objects(ctx, t, bucketName, files, s3Client)

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

	records, err = testutil.WaitForRecords(ctx, t, planTopic, kafkaBrokers, totalMapValues(expectedMap))
	require.NoError(t, err)

	checkPlannedEntries(t, expectedMap, records)
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

func createS3Objects(ctx context.Context, t *testing.T, bucketName string, files []string, s3Client *s3.Client) {
	t.Helper()
	for _, f := range files {
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f),
			Body:   strings.NewReader("dummy content"),
		})
		require.NoError(t, err)
	}
}
