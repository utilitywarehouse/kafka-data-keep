package planrestore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
)

type AppConfig struct {
	KafkaConfig kafka.Config
	internal.OpsConfig
	RestoreTopicsRegex string
	ExcludeTopicsRegex string
	PlanTopic          string
	S3                 ints3.Config
	S3Prefix           string
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3.Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}

	s3Client, err := ints3.NewClient(ctx, cfg.S3.Region, cfg.S3.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	kafkaClient, err := initKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer kafkaClient.Close()

	latestReader, err := kafka.NewLatestReader(cfg.KafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to create latest reader: %w", err)
	}
	defer latestReader.Close()

	slog.InfoContext(ctx, "Starting plan restore application...")

	planner := planner{
		s3Client:     s3Client,
		kafkaClient:  kafkaClient,
		latestReader: latestReader,
		cfg:          cfg,
	}
	return planner.Run(ctx)
}

func initKafkaClient(ctx context.Context, cfg AppConfig) (*kgo.Client, error) {
	opts, err := kafka.BaseOpts(cfg.KafkaConfig)
	if err != nil {
		return nil, err
	}

	opts = append(opts, []kgo.Opt{
		kgo.DefaultProduceTopic(cfg.PlanTopic),
	}...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return client, err
	}
	if err := client.Ping(ctx); err != nil {
		return client, fmt.Errorf("failed pinging kafka: %w", err)
	}
	return client, nil
}
