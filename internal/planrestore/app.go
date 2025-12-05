package planrestore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
	"log/slog"
)

type AppConfig struct {
	Brokers       string
	BrokersDNSSrv string
	RestoreTopics []string
	PlanTopic     string
	S3Bucket      string
	S3Endpoint    string
	S3Region      string
	S3Prefix      string
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

	producer, err := initKafkaProducer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer producer.Close()

	slog.InfoContext(ctx, "Starting plan restore application...")

	planner := Planner{
		s3Client: s3Client,
		producer: nil,
		cfg:      cfg,
	}
	return planner.Run(ctx)
}

func initKafkaProducer(cfg AppConfig) (*kafka.Client, error) {
	opts := []kgo.Opt{
		kafka.WithTracer(nil), // do not record traces
		kgo.DefaultProduceTopic(cfg.PlanTopic),
	}
	if cfg.BrokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv))
	} else {
		opts = append(opts, kgo.SeedBrokers(cfg.Brokers))
	}

	return kafka.NewClient(opts...)
}
