package restore

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

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
	PlanTopic          string
	RestoreTopicPrefix string
	ConsumerGroup      string
	S3Bucket           string
	S3Endpoint         string
	S3Region           string
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}

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
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer kafkaClient.CloseAllowingRebalance()

	slog.InfoContext(ctx, "Starting restore application...")

	seedBrokers := kafkaClient.OptValue(kgo.SeedBrokers).([]string)
	tlsConfig := kafkaClient.OptValue(kgo.DialTLSConfig).(*tls.Config)
	latestReader, err := kafkaint.NewLatestReader(seedBrokers, tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to create latest reader: %w", err)
	}
	defer latestReader.Close()

	restorer := kafkaS3Restorer{
		s3Client:     s3Client,
		cfg:          cfg,
		latestReader: latestReader,
		kafkaClient:  kafkaClient,
	}
	return restorer.Run(ctx)
}

func initKafkaClient(ctx context.Context, cfg AppConfig) (*kgo.Client, error) {
	opts, err := kafkaint.BaseOpts(cfg.KafkaConfig)
	if err != nil {
		return nil, err
	}

	opts = append(opts,
		kgo.ConsumeTopics(cfg.PlanTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // start from the beginning of topics
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.SessionTimeout(1*time.Minute),              // session timeout is 1 minute
		kgo.HeartbeatInterval(15*time.Second),          // increase heartbeat interval to 15 seconds, as processing a record is slow (~1s / record)
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // use a manual partitioner, as all the restore records have a partition set, and we should keep the same partition
		// Block rebalance while processing to not have partitions reallocated while processing a batch
		kgo.BlockRebalanceOnPoll(),
		// Use marking of records with autocommit. The marked records are auto committed on the next poll call.
		kgo.AutoCommitMarks(),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return client, err
	}
	if err := client.Ping(ctx); err != nil {
		return client, fmt.Errorf("failed pinging kafka: %w", err)
	}
	return client, nil
}
