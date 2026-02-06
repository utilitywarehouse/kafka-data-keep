package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

const (
	MinioRegion = "us-east-1"
	S3User      = "uwadmin"
	s3pass      = "uwadminpass"
)

func StartS3Service(t *testing.T) (string, func()) {
	t.Helper()
	ctx := t.Context()

	minioContainer, err := minio.Run(ctx, "minio/minio:latest", minio.WithUsername(S3User), minio.WithPassword(s3pass))
	require.NoError(t, err)

	terminateFunc := func() {
		// use the background context to terminate, as the test context is cancelled already at this point
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate MinIO container: %v", err)
		}
	}

	// Get connection details for MinIO
	connString, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)
	return fmt.Sprintf("http://%s", connString), terminateFunc
}

func NewS3Client(t *testing.T, s3Endpoint string) *s3.Client {
	t.Helper()
	awsCfg, err := config.LoadDefaultConfig(t.Context())
	require.NoError(t, err)

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})
}

func SetupEnvS3Access() {
	_ = os.Setenv("AWS_ACCESS_KEY_ID", S3User)
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", s3pass)
	_ = os.Setenv("AWS_REGION", MinioRegion)
}

func FileKey(s3Prefix string, topic string, partition int, offset int) string {
	filename := fmt.Sprintf("%s-%d-%s.avro", topic, partition, fmt.Sprintf("%019d", offset))
	fileKey := filepath.Join(s3Prefix, topic, fmt.Sprintf("%d", partition), filename)
	return fileKey
}
