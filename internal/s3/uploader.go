package s3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

// Uploader handles uploading files to S3.
type Uploader struct {
	client transfermanager.S3APIClient
	bucket string
}

// NewUploader creates a new Uploader.
func NewUploader(client transfermanager.S3APIClient, bucket string) *Uploader {
	return &Uploader{
		client: client,
		bucket: bucket,
	}
}

// Upload uploads a local file to S3.
func (u *Uploader) Upload(ctx context.Context, localPath, key string) (err error) {
	f, err := os.Open(filepath.Clean(localPath))
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", localPath, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close file: %w", closeErr)
		}
	}()

	tm := transfermanager.New(u.client)
	_, err = tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to s3://%s/%s: %w", u.bucket, key, err)
	}

	return nil
}
