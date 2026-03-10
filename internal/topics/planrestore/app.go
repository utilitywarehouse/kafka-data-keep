package planrestore

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
)

type AppConfig struct {
	KafkaConfig kafkaint.Config
	internal.OpsConfig
	RestoreTopicsRegex string
	ExcludeTopicsRegex string
	PlanTopic          string
	S3Bucket           string
	S3Endpoint         string
	S3Region           string
	S3Prefix           string
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}

	// Initialise S3 client and uploader
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.S3Region))
	if err != nil {
		return fmt.Errorf("unable to load SDK config: %w", err)
	}

	// Create S3 client with path-style addressing if using custom endpoint
	var s3ClientOpts []func(*s3.Options)
	if cfg.S3Endpoint != "" {
		s3ClientOpts = append(s3ClientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3ClientOpts...)

	kafkaClient, err := initKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer kafkaClient.Close()

	seedBrokers := kafkaClient.OptValue(kgo.SeedBrokers).([]string)    //nolint:errcheck // this would fail only if the franz-go lib changes, and we'll catch that in integration tests
	tlsConfig := kafkaClient.OptValue(kgo.DialTLSConfig).(*tls.Config) //nolint:errcheck // this would fail only if the franz-go lib changes, and we'll catch that in integration tests
	latestReader, err := kafkaint.NewLatestReader(seedBrokers, tlsConfig)
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
	opts, err := kafkaint.BaseOpts(cfg.KafkaConfig)
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
