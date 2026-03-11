package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/codec/avro"
	"golang.org/x/sync/errgroup"
	"net/http"
)

type AppConfig struct {
	KafkaConfig kafka.Config
	internal.OpsConfig
	TopicsRegex            string
	ExcludeTopicsRegex     string
	GroupID                string
	MinFileSize            int64
	PartitionIdleThreshold time.Duration
	WorkingDir             string
	S3                     ints3.Config
	S3Prefix               string
	EnableFlushOnSignal    bool
	EnableFlushServer      bool
	FlushServerPort        string
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3.Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}

	s3Client, err := ints3.NewClient(ctx, cfg.S3.Region, cfg.S3.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}
	uploader := ints3.NewUploader(s3Client, cfg.S3.Bucket)

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

	if cfg.EnableFlushServer {
		m := http.NewServeMux()
		m.HandleFunc("/__/flush", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			slog.InfoContext(r.Context(), "Received HTTP flush request, flushing all partition writers")
			if err := mgr.FlushAll(r.Context()); err != nil {
				slog.ErrorContext(r.Context(), "Failed to flush partition writers via HTTP", "error", err)
				http.Error(w, "Failed to flush", http.StatusInternalServerError)
				return
			}
			slog.InfoContext(r.Context(), "Successfully flushed all partition writers via HTTP")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Flushed\n"))
		})

		// running only on localhost, so it cannot be abused remotely
		addr := fmt.Sprintf("127.0.0.1:%s", cfg.FlushServerPort)
		closeFlushServer := internal.RunHTTPServer(ctx, addr, m, "flush server")
		defer closeFlushServer()
	}

	err = eg.Wait()
	slog.InfoContext(ctx, "Backup application exiting .... running cleanup", "error", err)
	return err
}

func initKafkaClient(ctx context.Context, cfg AppConfig, mgr *partitionsWriterManager) (*kgo.Client, error) {
	opts, err := kafka.BaseOpts(cfg.KafkaConfig)
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
