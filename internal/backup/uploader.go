package backup

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Uploader handles uploading files to S3.
type Uploader struct {
	client *s3.Client
	bucket string
}

// NewUploader creates a new Uploader.
func NewUploader(client *s3.Client, bucket string) *Uploader {
	return &Uploader{
		client: client,
		bucket: bucket,
	}
}

// Upload uploads a local file to S3.
func (u *Uploader) Upload(ctx context.Context, localPath, key string) (err error) {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", localPath, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close file: %w", closeErr)
		}
	}()

	uploader := manager.NewUploader(u.client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to s3://%s/%s: %w", u.bucket, key, err)
	}

	return nil
}

// FileExists checks if a file exists in S3.
func (u *Uploader) FileExists(ctx context.Context, key string) (bool, error) {
	_, err := u.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file exists s3://%s/%s: %w", u.bucket, key, err)
	}
	return true, nil
}
