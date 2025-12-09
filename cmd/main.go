package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/utilitywarehouse/go-operational/op"
	"github.com/utilitywarehouse/kafka-data-keep/internal/backup"
	"github.com/utilitywarehouse/kafka-data-keep/internal/planrestore"
	"github.com/utilitywarehouse/kafka-data-keep/internal/restore"
	"github.com/utilitywarehouse/uwos-go/telemetry"
	"github.com/utilitywarehouse/uwos-go/telemetry/log"
	"github.com/utilitywarehouse/uwos-go/x/build"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := mainWrap(); err != nil {
		slog.Error("app error", "error", err)
		os.Exit(1)
	}
}

func mainWrap() error {
	slog.Info(
		"Running version",
		slog.String("version", build.Version()),
	)

	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if len(os.Args) < 2 {
		return fmt.Errorf("expected subcommand")
	}

	switch os.Args[1] {
	case "backup":
		return runCmd(ctx, os.Args[2:], true, backupCmd)
	case "plan-restore":
		return runCmd(ctx, os.Args[2:], false, planRestoreCmd)
	case "restore":
		return runCmd(ctx, os.Args[2:], true, restoreCmd)
	default:
		return fmt.Errorf("expected 'backup|plan-restore|restore' subcommand")
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
		&cfg.S3Bucket,
		"s3-bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where to store the backups",
	)
	fs.StringVar(
		&cfg.S3Prefix,
		"s3-prefix",
		getEnv("S3_PREFIX", ""),
		"The prefix to use for the backup files in S3",
	)
	fs.Int64Var(
		&cfg.MinFileSize,
		"min-file-size",
		getEnvInt64("MIN_FILE_SIZE", 5*1024*1024),
		"The minimum file size in bytes for each partition backup file",
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

const opsAddr = "0.0.0.0:8081"

func runCmd(ctx context.Context, args []string, startOpsServer bool, cmd func(context.Context, []string) error) error {
	shutdown, err := telemetry.Register(ctx)
	if err != nil {
		return fmt.Errorf("failed registering telemetry services, err: %w", err)
	}

	defer func() {
		_ = shutdown.Close()
	}()

	logger := log.New()
	slog.SetDefault(logger)

	eg, ctx := errgroup.WithContext(ctx)

	if startOpsServer {
		eg.Go(func() error {
			opStatus := op.NewStatus(build.ServiceName, "kafka data keep").
				WithInstrumentedChecks().
				ReadyAlways()

			return runOpsServer(ctx, opsAddr, opStatus)
		})
	}

	eg.Go(func() error {
		return cmd(ctx, args)
	})

	return eg.Wait()
}

// Starts the operational server on the specified address expected in the format host:port and will stop it when the provided context is done.
func runOpsServer(ctx context.Context, operationalAddr string, opStatus *op.Status) error {
	opServer := &http.Server{
		Addr:              operationalAddr,
		Handler:           op.NewHandler(opStatus),
		ReadHeaderTimeout: 10 * time.Second,
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		slog.InfoContext(ctx, "operational server listening", "addr", operationalAddr)
		if err := opServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serving operational server: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		<-ctx.Done()
		slog.InfoContext(ctx, "stopping operational server")
		sCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		return opServer.Shutdown(sCtx)
	})
	return eg.Wait()
}

func backupCmd(ctx context.Context, args []string) error {
	cfg, err := loadBackupAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing backup config: %w", err)
	}

	if err := backup.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running backup: %w", err)
	}
	return nil
}

func planRestoreCmd(ctx context.Context, args []string) error {
	cfg, err := loadPlanRestoreAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing plan-restore config: %w", err)
	}

	if err := planrestore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running plan-restore: %w", err)
	}
	return nil
}

func loadPlanRestoreAppConfig(args []string) (planrestore.AppConfig, error) {
	var cfg planrestore.AppConfig
	fs := flag.NewFlagSet("plan-restore", flag.ExitOnError)

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

	fs.StringVar(
		&cfg.RestoreTopics,
		"restore-topics",
		getEnv("RESTORE_TOPICS", ""),
		"List of kafka topics to restore (comma separated)",
	)

	fs.StringVar(
		&cfg.PlanTopic,
		"plan-topic",
		getEnv("PLAN_TOPIC", "pubsub.plan-topic-restore"),
		"Kafka topic to send the restore plan to",
	)

	fs.StringVar(
		&cfg.S3Bucket,
		"s3-bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where the backup files are stored",
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
	fs.StringVar(
		&cfg.S3Prefix,
		"s3-prefix",
		getEnv("S3_PREFIX", "msk-backup"),
		"The prefix for the backup files in S3",
	)

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func restoreCmd(ctx context.Context, args []string) error {
	cfg, err := loadRestoreAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing restore config: %w", err)
	}

	if err := restore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running restore: %w", err)
	}
	return nil
}

func loadRestoreAppConfig(args []string) (restore.AppConfig, error) {
	var cfg restore.AppConfig
	fs := flag.NewFlagSet("restore", flag.ExitOnError)

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
		&cfg.PlanTopic,
		"plan-topic",
		getEnv("KAFKA_PLAN_TOPIC", "pubsub.plan-topic-restore"),
		"Kafka topic to consume the plan from",
	)
	fs.StringVar(
		&cfg.RestoreTopicPrefix,
		"restore-topic-prefix",
		getEnv("KAFKA_RESTORE_TOPIC_PREFIX", "pubsub.restore-test."),
		"Prefix to add to the restored topics",
	)
	fs.StringVar(
		&cfg.ConsumerGroup,
		"consumer-group",
		getEnv("KAFKA_CONSUMER_GROUP", "pubsub.msk-data-keep-restore"),
		"Kafka consumer group ID",
	)

	// Storage
	fs.StringVar(
		&cfg.S3Bucket,
		"s3-bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where the backups are stored",
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
