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
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/utilitywarehouse/go-operational/op"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	consumergroupsbackup "github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/backup"
	consumergroupsrestore "github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/restore"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	topicsbackup "github.com/utilitywarehouse/kafka-data-keep/internal/topics/backup"
	topicsplanrestore "github.com/utilitywarehouse/kafka-data-keep/internal/topics/planrestore"
	topicsrestore "github.com/utilitywarehouse/kafka-data-keep/internal/topics/restore"
	"github.com/utilitywarehouse/uwos-go/x/build"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := mainWrap(); err != nil {
		slog.Error("app error", "error", err)
		os.Exit(1)
	}
}

func mainWrap() error {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		slog.Info("Running app", "build_info", buildInfo.String())
	}

	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if len(os.Args) < 2 {
		return fmt.Errorf("expected subcommand")
	}

	switch os.Args[1] {
	case "topics-backup":
		return runCmd(ctx, os.Args[2:], true, topicsBackupCmd)
	case "topics-plan-restore":
		return runCmd(ctx, os.Args[2:], false, topicsPlanRestoreCmd)
	case "topics-restore":
		return runCmd(ctx, os.Args[2:], true, topicsRestoreCmd)
	case "consumer-groups-backup":
		return runCmd(ctx, os.Args[2:], true, consumerGroupsBackupCmd)
	case "consumer-groups-restore":
		return runCmd(ctx, os.Args[2:], false, consumerGroupsRestoreCmd)
	default:
		return fmt.Errorf("expected 'topics-backup|topics-plan-restore|topics-restore|consumer-groups-backup|consumer-groups-restore' subcommand")
	}
}

func loadTopicsBackupAppConfig(args []string) (topicsbackup.AppConfig, error) {
	var cfg topicsbackup.AppConfig
	fs := flag.NewFlagSet("topics-backup", flag.ExitOnError)
	bindKafkaConfig(fs, &cfg.Config)
	bindLogConfig(fs, &cfg.LogConfig)

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
	fs.DurationVar(
		&cfg.PartitionIdleThreshold,
		"partition-idle-threshold",
		getEnvDuration("PARTITION_IDLE_THRESHOLD", 1*time.Minute),
		"The threshold after which a partition will be considered idle for not consuming any new records. Should be a duration",
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
	cfg.EnableFlushOnSignal = true
	return cfg, nil
}

const opsAddr = "0.0.0.0:8081"

func runCmd(ctx context.Context, args []string, startOpsServer bool, cmd func(context.Context, []string) error) error {
	err := initOtelMetrics()
	if err != nil {
		return err
	}

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

var metricInit sync.Once

func initOtelMetrics() error {
	// using the prometheus exporter
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(prometheus.DefaultRegisterer),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return fmt.Errorf("failed initializing prometheus exporter: %w", err)
	}
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(exporter)))

	var metricInitErr error
	metricInit.Do(func() {
		// We use OpenTelemetry runtime/go metrics
		// go.opentelemetry.io/contrib/instrumentation/runtime
		//
		// As they can be expensive to collect, we disable the Prometheus
		// client also registering the same metrics
		prometheus.Unregister(collectors.NewGoCollector())

		metricInitErr = runtime.Start(runtime.WithMinimumReadMemStatsInterval(10 * time.Second))
	})

	if metricInitErr != nil {
		return fmt.Errorf("telemetry: failed to start runtime metric instrumentation: %w", metricInitErr)
	}

	return nil
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

func topicsBackupCmd(ctx context.Context, args []string) error {
	cfg, err := loadTopicsBackupAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing backup config: %w", err)
	}

	internal.InitGlobalLog(cfg.LogConfig)
	if err := topicsbackup.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running backup: %w", err)
	}
	return nil
}

func topicsPlanRestoreCmd(ctx context.Context, args []string) error {
	cfg, err := loadTopicsPlanRestoreAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing plan-restore config: %w", err)
	}

	internal.InitGlobalLog(cfg.LogConfig)

	if err := topicsplanrestore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running plan-restore: %w", err)
	}
	return nil
}

func loadTopicsPlanRestoreAppConfig(args []string) (topicsplanrestore.AppConfig, error) {
	var cfg topicsplanrestore.AppConfig
	fs := flag.NewFlagSet("topics-plan-restore", flag.ExitOnError)

	bindKafkaConfig(fs, &cfg.Config)
	bindLogConfig(fs, &cfg.LogConfig)

	fs.StringVar(
		&cfg.RestoreTopicsRegex,
		"restore-topics-regex",
		getEnv("RESTORE_TOPICS_REGEX", ".*"),
		"List of regex to match topics to restore (comma separated). The topics will be restored in the order specified in this list",
	)
	fs.StringVar(
		&cfg.ExcludeTopicsRegex,
		"exclude-topics-regex",
		getEnv("EXCLUDE_TOPICS_REGEX", ""),
		"List of regex to exclude topics from restore (comma separated)",
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

func topicsRestoreCmd(ctx context.Context, args []string) error {
	cfg, err := loadTopicsRestoreAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing restore config: %w", err)
	}

	internal.InitGlobalLog(cfg.LogConfig)

	if err := topicsrestore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running restore: %w", err)
	}
	return nil
}

func loadTopicsRestoreAppConfig(args []string) (topicsrestore.AppConfig, error) {
	var cfg topicsrestore.AppConfig
	fs := flag.NewFlagSet("topics-restore", flag.ExitOnError)

	bindKafkaConfig(fs, &cfg.Config)
	bindLogConfig(fs, &cfg.LogConfig)

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
		"group-id",
		getEnv("KAFKA_GROUP_ID", "pubsub.msk-data-keep-restore"),
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

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		i, err := time.ParseDuration(value)
		if err != nil {
			return fallback
		}
		return i
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fallback
		}
		return b
	}
	return fallback
}

func consumerGroupsBackupCmd(ctx context.Context, args []string) error {
	cfg, err := loadConsumerGroupsBackupAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing consumer-groups-backup config: %w", err)
	}

	internal.InitGlobalLog(cfg.LogConfig)

	if err := consumergroupsbackup.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running consumer-groups-backup: %w", err)
	}
	return nil
}

func loadConsumerGroupsBackupAppConfig(args []string) (consumergroupsbackup.AppConfig, error) {
	var cfg consumergroupsbackup.AppConfig
	fs := flag.NewFlagSet("consumer-groups-backup", flag.ExitOnError)

	bindKafkaConfig(fs, &cfg.Config)
	bindLogConfig(fs, &cfg.LogConfig)

	fs.StringVar(
		&cfg.S3Bucket,
		"s3-bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where to store the backups",
	)
	fs.StringVar(
		&cfg.S3Location,
		"s3-location",
		getEnv("S3_LOCATION", ""),
		"The s3 location (full path key) to use for the backup file",
	)

	fs.DurationVar(
		&cfg.RunInterval,
		"run-interval",
		getEnvDuration("RUN_INTERVAL", 1*time.Minute),
		"Interval between backups",
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

func consumerGroupsRestoreCmd(ctx context.Context, args []string) error {
	cfg, err := loadConsumerGroupsRestoreAppConfig(args)
	if err != nil {
		return fmt.Errorf("failed parsing consumer-groups-restore config: %w", err)
	}

	internal.InitGlobalLog(cfg.LogConfig)

	if err := consumergroupsrestore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("error running consumer-groups-restore: %w", err)
	}
	return nil
}

func loadConsumerGroupsRestoreAppConfig(args []string) (consumergroupsrestore.AppConfig, error) {
	var cfg consumergroupsrestore.AppConfig
	fs := flag.NewFlagSet("consumer-groups-restore", flag.ExitOnError)

	bindKafkaConfig(fs, &cfg.Config)
	bindLogConfig(fs, &cfg.LogConfig)

	fs.StringVar(
		&cfg.S3Bucket,
		"s3-bucket",
		getEnv("S3_BUCKET", ""),
		"S3 bucket name where the consumer groups backup is stored",
	)
	fs.StringVar(
		&cfg.S3Location,
		"s3-location",
		getEnv("S3_LOCATION", ""),
		"The s3 location (full path key) of the consumer groups backup file",
	)

	fs.StringVar(
		&cfg.RestoreGroupsPrefix,
		"restore-groups-prefix",
		getEnv("RESTORE_GROUPS_PREFIX", ""),
		"Prefix to add to the restored consumer group names",
	)
	fs.StringVar(
		&cfg.RestoreTopicsPrefix,
		"restore-topics-prefix",
		getEnv("RESTORE_TOPICS_PREFIX", ""),
		"Prefix used on the restored topic names",
	)
	fs.StringVar(
		&cfg.IncludeRegexes,
		"include-regexes",
		getEnv("INCLUDE_REGEXES", ".*"),
		"List of regular expressions to match consumer groups to restore (comma separated)",
	)
	fs.StringVar(
		&cfg.ExcludeRegexes,
		"exclude-regexes",
		getEnv("EXCLUDE_REGEXES", ""),
		"List of regular expressions to exclude consumer groups from restore (comma separated)",
	)

	fs.DurationVar(
		&cfg.LoopInterval,
		"loop-interval",
		getEnvDuration("LOOP_INTERVAL", 1*time.Minute),
		"Duration between consumer group restore iterations",
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

func bindKafkaConfig(fs *flag.FlagSet, cfg *kafka.Config) {
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
		"Convenient way of passing the seed brokers in a single DNS SRV record with the service name being \"kafka\"",
	)
	fs.BoolVar(
		&cfg.MTLSAuth,
		"kafka-mtls-auth",
		getEnvBool("KAFKA_MTLS_AUTH", false),
		"Kafka cluster uses mTLS authentication",
	)
	fs.StringVar(
		&cfg.MTLSCA,
		"kafka-mtls-ca-cert-path",
		getEnv("KAFKA_MTLS_CA_CERT_PATH", "/certs/ca.crt"),
		"The path of the file containing the CA cert",
	)
	fs.StringVar(
		&cfg.MTLSCert,
		"kafka-mtls-client-cert-path",
		getEnv("KAFKA_MTLS_CLIENT_CERT_PATH", "/certs/tls.crt"),
		"The path of the file containing the client cert",
	)
	fs.StringVar(
		&cfg.MTLSKey,
		"kafka-mtls-client-key-path",
		getEnv("KAFKA_MTLS_CLIENT_KEY_PATH", "/certs/tls.key"),
		"The path of the file containing the client private key",
	)
}

func bindLogConfig(fs *flag.FlagSet, cfg *internal.LogConfig) {
	fs.StringVar(
		&cfg.LogLevel,
		"log-level",
		getEnv("LOG_LEVEL", "INFO"),
		"The log level to use",
	)
	fs.StringVar(
		&cfg.KGOLogLevel,
		"kgo-log-level",
		getEnv("KGO_LOG_LEVEL", "INFO"),
		"The log level for the franz-go library",
	)
	fs.StringVar(
		&cfg.LogFormat,
		"log-format",
		getEnv("LOG_FORMAT", "text"),
		"The log format to use (text, json)",
	)
}
