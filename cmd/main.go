package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/avro"
	"github.com/utilitywarehouse/kafka-data-keep/internal/backup"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

type BackupAppConfig struct {
	Brokers            string
	BrokersDNSSrv      string
	TopicsRegex        string
	ExcludeTopicsRegex string
	GroupID            string
	Bucket             string
	FileSize           int64
	WorkingDir         string
	S3Endpoint         string
	S3Region           string
}

func loadBackupAppConfig() BackupAppConfig {
	var cfg BackupAppConfig

	// Kafka Connection
	flag.StringVar(
		&cfg.Brokers,
		"brokers",
		getEnv("KAFKA_BROKERS", "localhost:9092"),
		"Kafka brokers (comma separated)",
	)
	flag.StringVar(
		&cfg.BrokersDNSSrv,
		"brokersDNSSrv",
		getEnv("KAFKA_BROKERS_DNS_SRV", ""),
		"DNS SRV record with the kafka seed brokers",
	)

	// Kafka Consumer
	flag.StringVar(
		&cfg.TopicsRegex,
		"topics-regex",
		getEnv("KAFKA_TOPICS_REGEX", ".*"),
		"List of kafka topics regex to consume (comma separated)",
	)
	flag.StringVar(
		&cfg.ExcludeTopicsRegex,
		"exclude-topics-regex",
		getEnv("KAFKA_EXCLUDE_TOPICS_REGEX", ""),
		"List of kafka topics regex to exclude from consuming (comma separated)",
	)
	flag.StringVar(
		&cfg.GroupID,
		"group-id",
		getEnv("KAFKA_GROUP_ID", "kafka-data-keep"),
		"Kafka consumer group ID",
	)

	// Storage
	flag.StringVar(
		&cfg.Bucket,
		"bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where to store the backups",
	)
	flag.Int64Var(
		&cfg.FileSize,
		"file-size",
		getEnvInt64("FILE_SIZE", 5*1024*1024),
		"File size in bytes",
	)
	flag.StringVar(
		&cfg.WorkingDir,
		"working-dir",
		getEnv("WORKING_DIR", "kafka-backup-data"),
		"Working directory for local files",
	)
	flag.StringVar(
		&cfg.S3Endpoint,
		"s3-endpoint",
		getEnv("AWS_ENDPOINT_URL", ""),
		"S3 endpoint URL (for LocalStack or custom S3-compatible storage)",
	)
	flag.StringVar(
		&cfg.S3Region,
		"s3-region",
		getEnv("AWS_REGION", "eu-west-1"),
		"S3 region ",
	)

	flag.Parse()
	return cfg
}

func main() {
	cfg := loadBackupAppConfig()

	if cfg.Bucket == "" {
		slog.Error("bucket must be provided")
		os.Exit(1)
	}

	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize S3 client and uploader
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.S3Region))
	if err != nil {
		slog.Error("unable to load SDK config", "error", err)
		os.Exit(1)
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
	uploader := backup.NewUploader(s3Client, cfg.Bucket)

	// Create working dir for local files
	if err := os.MkdirAll(cfg.WorkingDir, 0755); err != nil {
		slog.Error("failed to create working dir", "error", err)
		os.Exit(1)
	}
	slog.Info("Using working dir for local files", "path", cfg.WorkingDir)

	wConfig := backup.Config{
		FileSize: cfg.FileSize,
		RootPath: cfg.WorkingDir,
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

	client, err := initKafkaClient(cfg, mgr)
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

const maxPollRecords = 10000 // this affects how many records are processed per poll, not how many are fetched from Kafka
func initKafkaClient(cfg BackupAppConfig, mgr *backup.PartitionsWriterManager) (*kafka.Client, error) {
	opts := []kgo.Opt{
		kgo.ConsumeRegex(), // use regex to consume topics
		kgo.ConsumeTopics(strings.Split(cfg.TopicsRegex, ",")...),
		kgo.ConsumeExcludeTopics(strings.Split(cfg.ExcludeTopicsRegex, ",")...),
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
			mgr.OnPartitionsRevoked(p)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, p map[string][]int32) {
			mgr.OnPartitionLost(p)
		}),
	}
	if cfg.BrokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv))
	} else {
		opts = append(opts, kgo.SeedBrokers(cfg.Brokers))
	}
	return kafka.NewClient(opts...)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fallback
		}
		return i
	}
	return fallback
}
