package backup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec/avro"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
)

type AppConfig struct {
	KafkaConfig kafka.Config
	internal.OpsConfig
	S3          ints3.Config
	S3Location  string
	RunInterval time.Duration
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3.Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}
	if cfg.S3Location == "" {
		return fmt.Errorf("s3-location must be provided")
	}

	s3Client, err := ints3.NewClient(ctx, cfg.S3.Region, cfg.S3.Endpoint)
	if err != nil {
		return fmt.Errorf("failed creating s3 client: %w", err)
	}
	uploader := ints3.NewUploader(s3Client, cfg.S3.Bucket)

	client, err := initKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()

	kadmClient := kadm.NewClient(client)
	defer kadmClient.Close()

	encFactory := &avro.GroupEncoderFactory{}
	groupWriter := NewGroupWriter(kadmClient, uploader, encFactory, cfg.S3Location)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := groupWriter.Backup(ctx); err != nil {
				slog.Error("failed to backup consumer groups", "error", err)
			}
		}
	}
}

func initKafkaClient(ctx context.Context, cfg AppConfig) (*kgo.Client, error) {
	opts, err := kafka.BaseOpts(cfg.KafkaConfig)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return client, err
	}
	if err := client.Ping(ctx); err != nil {
		return client, fmt.Errorf("failed pinging kafka: %w", err)
	}
	return client, nil
}
