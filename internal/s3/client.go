package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds the common S3 configuration fields used by all commands.
type Config struct {
	Bucket   string
	Endpoint string
	Region   string
}

// NewClient initializes an S3 client based on the provided region and endpoint.
// If endpoint is not empty, it configures the client to use path-style addressing.
func NewClient(ctx context.Context, region, endpoint string) (*s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	var s3ClientOpts []func(*s3.Options)
	if endpoint != "" {
		s3ClientOpts = append(s3ClientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(awsCfg, s3ClientOpts...), nil
}
