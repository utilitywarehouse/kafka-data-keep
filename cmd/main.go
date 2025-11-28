package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/avro"
	"github.com/utilitywarehouse/kafka-data-keep/internal/backup"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

func main() {
	var (
		brokers            = flag.String("brokers", "localhost:9092", "Kafka brokers (comma separated)")
		brokersDNSSrv      = flag.String("brokersDNSSrv", "", "DNS SRV record with the kafka seed brokers")
		topicsRegex        = flag.String("topics-regex", "*", "List of kafka topics regex to consume (comma separated)")
		excludeTopicsRegex = flag.String("exclude-topics-regex", "", "List of kafka topics regex to exclude from consuming (comma separated)")
		groupID            = flag.String("group-id", "kafka-data-keep", "Kafka consumer group ID")
		bucket             = flag.String("bucket", "", "S3 bucket name where to store the backups")
		fileSize           = flag.Int64("file-size", 5*1024*1024, "File size in bytes")
	)
	flag.Parse()

	if *bucket == "" {
		slog.Error("bucket must be provided")
		os.Exit(1)
	}

	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize S3 client and uploader
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error("unable to load SDK config", "error", err)
		os.Exit(1)
	}
	s3Client := s3.NewFromConfig(cfg)
	uploader := backup.NewUploader(s3Client, *bucket)

	// Create a temp dir for local files
	tmpDir, err := os.MkdirTemp("", "kafka-backup")
	if err != nil {
		slog.Error("failed to create temp dir", "error", err)
		os.Exit(1)
	}
	slog.Info("Using temp dir for local files", "path", tmpDir)
	// Note: we should probably clean up this temp dir on exit, but the files are deleted after upload anyway.

	wConfig := backup.Config{
		FileSize: *fileSize,
		RootPath: tmpDir,
	}

	// Create manager first

	mgr, err := backup.NewPartitionsWriterManager(uploader, &avro.RecordEncoderFactory{}, wConfig)
	if err != nil {
		slog.Error("failed to create writer manager", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := mgr.Close(); err != nil {
			slog.Error("failed to close manager", "error", err)
		}
	}()

	// Initialize Kafka client
	const maxPollRecords = 10000 // this affects how many records are processed per poll, not how many are fetched from Kafka
	opts := []kgo.Opt{
		kgo.ConsumeRegex(), // use regex to consume topics
		kgo.ConsumeTopics(strings.Split(*topicsRegex, ",")...),
		kgo.ConsumeExcludeTopics(strings.Split(*excludeTopicsRegex, ",")...),
		kafka.WithMaxPollRecords(maxPollRecords),
		kafka.WithConsumeOldestOffset(),
		kafka.WithTracer(nil), // do not record traces
		kgo.ConsumerGroup(*groupID),
		kgo.DisableAutoCommit(),    // We will commit manually
		kgo.BlockRebalanceOnPoll(), // block rebalance while processing records
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionsAssigned(c, p)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionsRevoked(p)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionLost(p)
		}),
	}
	if *brokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(*brokersDNSSrv))
	} else {
		opts = append(opts, kgo.SeedBrokers(*brokers))
	}
	client, err := kafka.NewClient(opts...)
	if err != nil {
		slog.Error("failed to create kafka client", "error", err)
		os.Exit(1)
	}
	defer client.CloseAllowingRebalance()

	slog.Info("Starting backup application...")
	if err := backup.Run(ctx, client, mgr); err != nil {
		slog.Error("consumer error", "error", err)
	}

}
