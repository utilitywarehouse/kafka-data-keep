package restore

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
)

type AppConfig struct {
	KafkaConfig kafka.Config
	internal.OpsConfig
	PlanTopic          string
	RestoreTopicPrefix string
	ConsumerGroup      string
	S3                 ints3.Config
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
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer kafkaClient.CloseAllowingRebalance()

	slog.InfoContext(ctx, "Starting restore application...")

	latestReader, err := kafka.NewLatestReader(cfg.KafkaConfig)
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
	opts, err := kafka.BaseOpts(cfg.KafkaConfig)
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
