package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"fmt"

	"github.com/utilitywarehouse/kafka-data-keep/internal/backup"
)

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

func loadBackupAppConfig(args []string) (backup.AppConfig, error) {
	var cfg backup.AppConfig
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

func backupCmd(ctx context.Context, args []string) error {
	cfg, err := loadBackupAppConfig(args)
	if err != nil {
		return err
	}

	if err := backup.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running backup: %w", err)
	}
	return nil
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
