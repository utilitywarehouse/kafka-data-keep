package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
	"golang.org/x/sync/errgroup"
)

type AppConfig struct {
	Brokers                string
	BrokersDNSSrv          string
	TopicsRegex            string
	ExcludeTopicsRegex     string
	GroupID                string
	MinFileSize            int64
	PartitionIdleThreshold time.Duration
	WorkingDir             string
	S3Bucket               string
	S3Endpoint             string
	S3Region               string
	S3Prefix               string
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
	uploader := NewUploader(s3Client, cfg.S3Bucket)

	// Create working dir for local files
	if err := os.MkdirAll(cfg.WorkingDir, 0o755); err != nil {
		return fmt.Errorf("failed to create working dir: %w", err)
	}
	slog.InfoContext(ctx, "Using working dir for local files", "path", cfg.WorkingDir)

	wConfig := Config{
		MinFileSize:            cfg.MinFileSize,
		PartitionIdleThreshold: cfg.PartitionIdleThreshold,
		RootPath:               cfg.WorkingDir,
		S3Prefix:               cfg.S3Prefix,
	}

	// Create manager first
	mgr, err := NewPartitionsWriterManager(uploader, &avro.RecordEncoderFactory{}, wConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer manager: %w", err)
	}
	defer func() {
		//nolint: contextcheck
		if err := mgr.Close(); err != nil {
			slog.ErrorContext(ctx, "failed to close manager", "error", err)
		}
	}()

	client, err := initKafkaClient(cfg, mgr)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.CloseAllowingRebalance()

	slog.InfoContext(ctx, "Starting backup application...")
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return runConsumer(ctx, client, mgr)
	})

	eg.Go(func() error {
		return runPauseIdleWriters(ctx, mgr)
	})

	return eg.Wait()
}

const maxPollRecords = 10000 // this affects how many records are processed per poll, not how many are fetched from Kafka
func initKafkaClient(cfg AppConfig, mgr *PartitionsWriterManager) (*kafka.Client, error) {
	opts := []kgo.Opt{
		kgo.ConsumeRegex(), // use regex to consume topics
		kgo.ConsumeTopics(splitAndTrim(cfg.TopicsRegex, ",")...),
		kafka.WithMaxPollRecords(maxPollRecords),
		kafka.WithConsumeOldestOffset(),
		kafka.WithTracer(nil), // do not record traces
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.DisableAutoCommit(),    // We will commit manually
		kgo.BlockRebalanceOnPoll(), // block rebalance while processing records
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionsAssigned(c, p)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionsRevoked(ctx, p)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionLost(ctx, p)
		}),
	}
	if cfg.BrokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv))
	} else {
		opts = append(opts, kgo.SeedBrokers(cfg.Brokers))
	}
	if cfg.ExcludeTopicsRegex != "" {
		opts = append(opts, kgo.ConsumeExcludeTopics(splitAndTrim(cfg.ExcludeTopicsRegex, ",")...))
	}

	return kafka.NewClient(opts...)
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
