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

	"fmt"

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

func loadBackupAppConfig(args []string) (BackupAppConfig, error) {
	var cfg BackupAppConfig
	fs := flag.NewFlagSet("backup", flag.ExitOnError)

	// Kafka Connection
	fs.StringVar(
		&cfg.Brokers,
		"brokers",
		getEnv("KAFKA_BROKERS", "localhost:9092"),
		"Kafka brokers (comma separated)",
	)
	fs.StringVar(
		&cfg.BrokersDNSSrv,
		"brokersDNSSrv",
		getEnv("KAFKA_BROKERS_DNS_SRV", ""),
		"DNS SRV record with the kafka seed brokers",
	)

	// Kafka Consumer
	fs.StringVar(
		&cfg.TopicsRegex,
		"topics-regex",
		getEnv("KAFKA_TOPICS_REGEX", ".*"),
		"List of kafka topics regex to consume (comma separated)",
	)
	fs.StringVar(
		&cfg.ExcludeTopicsRegex,
		"exclude-topics-regex",
		getEnv("KAFKA_EXCLUDE_TOPICS_REGEX", ""),
		"List of kafka topics regex to exclude from consuming (comma separated)",
	)
	fs.StringVar(
		&cfg.GroupID,
		"group-id",
		getEnv("KAFKA_GROUP_ID", "kafka-data-keep"),
		"Kafka consumer group ID",
	)

	// Storage
	fs.StringVar(
		&cfg.Bucket,
		"bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where to store the backups",
	)
	fs.Int64Var(
		&cfg.FileSize,
		"file-size",
		getEnvInt64("FILE_SIZE", 5*1024*1024),
		"File size in bytes for each partition backup file",
	)
	fs.StringVar(
		&cfg.WorkingDir,
		"working-dir",
		getEnv("WORKING_DIR", "kafka-backup-data"),
		"Working directory for local files",
	)
	fs.StringVar(
		&cfg.S3Endpoint,
		"s3-endpoint",
		getEnv("AWS_ENDPOINT_URL", ""),
		"S3 endpoint URL (for LocalStack or custom S3-compatible storage)",
	)
	fs.StringVar(
		&cfg.S3Region,
		"s3-region",
		getEnv("AWS_REGION", "eu-west-1"),
		"S3 region ",
	)

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func main() {
	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if len(os.Args) < 2 {
		fmt.Println("expected 'backup' subcommand")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "backup":
		if err := backupCmd(ctx, os.Args[2:]); err != nil {
			slog.Error("backup command failed", "error", err)
			os.Exit(1)
		}
	default:
		fmt.Println("expected 'backup' subcommand")
		os.Exit(1)
	}
}

func backupCmd(ctx context.Context, args []string) error {
	cfg, err := loadBackupAppConfig(args)
	if err != nil {
		return err
	}

	if err := runBackup(ctx, cfg); err != nil {
		return fmt.Errorf("error running backup: %w", err)
	}
	return nil
}

func runBackup(ctx context.Context, cfg BackupAppConfig) error {
	if cfg.Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}

	// Initialize S3 client and uploader
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
	uploader := backup.NewUploader(s3Client, cfg.Bucket)

	// Create working dir for local files
	if err := os.MkdirAll(cfg.WorkingDir, 0755); err != nil {
		return fmt.Errorf("failed to create working dir: %w", err)
	}
	slog.Info("Using working dir for local files", "path", cfg.WorkingDir)

	wConfig := backup.Config{
		FileSize: cfg.FileSize,
		RootPath: cfg.WorkingDir,
	}

	// Create manager first
	mgr, err := backup.NewPartitionsWriterManager(uploader, &avro.RecordEncoderFactory{}, wConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer manager: %w", err)
	}
	defer func() {
		if err := mgr.Close(); err != nil {
			slog.Error("failed to close manager", "error", err)
		}
	}()

	client, err := initKafkaClient(cfg, mgr)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.CloseAllowingRebalance()

	slog.Info("Starting backup application...")
	return backup.Run(ctx, client, mgr)
}

const maxPollRecords = 10000 // this affects how many records are processed per poll, not how many are fetched from Kafka
func initKafkaClient(cfg BackupAppConfig, mgr *backup.PartitionsWriterManager) (*kafka.Client, error) {
	opts := []kgo.Opt{
		kgo.ConsumeRegex(), // use regex to consume topics
		kgo.ConsumeTopics(strings.Split(cfg.TopicsRegex, ",")...),
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
	if cfg.ExcludeTopicsRegex != "" {
		opts = append(opts, kgo.ConsumeExcludeTopics(strings.Split(cfg.ExcludeTopicsRegex, ",")...))
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
