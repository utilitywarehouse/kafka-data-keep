package planrestore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
)

func TestListTopicsFromS3(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	s3Endpoint, ts3f := testutil.StartS3Service(ctx, t)
	defer ts3f()

	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(ctx, t, s3Endpoint)

	bucketName := "test-restore-bucket"
	// Create bucket
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	// Seed data
	s3Prefix := "restore-test"
	topics := []string{"topic-a", "topic-b", "topic-c"}

	for _, topic := range topics {
		key := fmt.Sprintf("%s/%s/0/000.avro", s3Prefix, topic)
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("dummy content"),
		})
		require.NoError(t, err)
	}

	planner := &Planner{
		s3Client: s3Client,
		cfg: AppConfig{
			S3Bucket: bucketName,
			S3Prefix: s3Prefix,
		},
	}

	gotTopics, err := planner.listTopicsFromS3(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, topics, gotTopics)
}
