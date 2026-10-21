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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	MinioRegion = "us-east-1"
	S3User      = "uwadmin"
	s3pass      = "uwadminpass"
)

func StartS3Service(t *testing.T) (string, func()) {
	t.Helper()
	ctx := t.Context()

	// LocalStack accepts any credentials; pinned to a release that doesn't require an auth token
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "localstack/localstack:3.8",
			ExposedPorts: []string{"4566/tcp"},
			Env:          map[string]string{"SERVICES": "s3"},
			WaitingFor:   wait.ForHTTP("/_localstack/health").WithPort("4566/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err)

	terminateFunc := func() {
		// use the background context to terminate, as the test context is cancelled already at this point
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate LocalStack container: %v", err)
		}
	}

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "4566/tcp")
	require.NoError(t, err)
	return fmt.Sprintf("http://%s:%s", host, port.Port()), terminateFunc
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
