package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/codec/avro"
	"golang.org/x/sync/errgroup"
)

type AppConfig struct {
	kafkaint.Config
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
	EnableFlushOnSignal    bool
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
	uploader := ints3.NewUploader(s3Client, cfg.S3Bucket)

	// Create working dir for local files
	if err := os.MkdirAll(cfg.WorkingDir, 0o750); err != nil {
		return fmt.Errorf("failed to create working dir: %w", err)
	}
	slog.InfoContext(ctx, "Using working dir for local files", "path", cfg.WorkingDir)

	wConfig := writerConfig{
		MinFileSize:            cfg.MinFileSize,
		PartitionIdleThreshold: cfg.PartitionIdleThreshold,
		RootPath:               cfg.WorkingDir,
		S3Prefix:               cfg.S3Prefix,
	}

	// Create manager first
	mgr, err := newPartitionsWriterManager(uploader, &avro.RecordEncoderFactory{}, &avro.RecordDecoderFactory{}, wConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer manager: %w", err)
	}
	defer func() {
		slog.InfoContext(ctx, "Closing partition manager ...")
		err := mgr.Close()
		slog.InfoContext(ctx, "Finished closing partition manager", "error", err)
	}()

	client, err := initKafkaClient(ctx, cfg, mgr)
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

	if cfg.EnableFlushOnSignal {
		eg.Go(func() error {
			return runFlushOnSignal(ctx, mgr)
		})
	}

	err = eg.Wait()
	slog.InfoContext(ctx, "Backup application exiting .... running cleanup", "error", err)
	return err
}

func initKafkaClient(ctx context.Context, cfg AppConfig, mgr *partitionsWriterManager) (*kgo.Client, error) {
	opts, err := kafkaint.BaseOpts(cfg.Config)
	if err != nil {
		return nil, err
	}

	opts = append(opts, []kgo.Opt{
		kgo.ConsumeRegex(), // use regex to consume topics
		kgo.ConsumeTopics(internal.SplitAndTrim(cfg.TopicsRegex, ",")...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.DisableAutoCommit(),    // We will commit manually
		kgo.BlockRebalanceOnPoll(), // block rebalance while processing records
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionsAssigned(ctx, c, p)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionsRevoked(ctx, p)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, _ *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionLost(ctx, p)
		}),
	}...)

	if cfg.ExcludeTopicsRegex != "" {
		opts = append(opts, kgo.ConsumeExcludeTopics(internal.SplitAndTrim(cfg.ExcludeTopicsRegex, ",")...))
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

func runPauseIdleWriters(ctx context.Context, pwManager *partitionsWriterManager) error {
	tickerMillis := min(pwManager.config.PartitionIdleThreshold.Milliseconds(), time.Minute.Milliseconds())
	slog.InfoContext(ctx, "Start pausing idle writers", "interval", tickerMillis)

	ticker := time.NewTicker(time.Duration(tickerMillis) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := pwManager.PauseIdleWriters(ctx); err != nil {
				return fmt.Errorf("failed pausing idle writers: %w", err)
			}
		}
	}
}

func runFlushOnSignal(ctx context.Context, pwManager *partitionsWriterManager) error {
	flushCh := make(chan os.Signal, 1)
	signal.Notify(flushCh, syscall.SIGUSR1)
	defer signal.Stop(flushCh)

	slog.InfoContext(ctx, "Listening for SIGUSR1 to flush partition writers")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-flushCh:
			slog.InfoContext(ctx, "Received SIGUSR1, flushing all partition writers")
			if err := pwManager.FlushAll(ctx); err != nil {
				slog.ErrorContext(ctx, "Failed to flush partition writers on signal", "error", err)
			} else {
				slog.InfoContext(ctx, "Successfully flushed all partition writers")
			}
		}
	}
}
