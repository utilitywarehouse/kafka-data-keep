package restore

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

type AppConfig struct {
	Brokers            string
	BrokersDNSSrv      string
	PlanTopic          string
	RestoreTopicPrefix string
	ConsumerGroup      string
	S3Bucket           string
	S3Endpoint         string
	S3Region           string
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}

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

	planConsumer, err := initKafkaConsumer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	defer planConsumer.Close()

	slog.InfoContext(ctx, "Starting restore application...")
	restorer := kafkaS3Restorer{
		consumer: planConsumer,
		s3Client: s3Client,
		cfg:      cfg,
	}
	return restorer.Run(ctx)
}

func initKafkaConsumer(cfg AppConfig) (*kafka.SimpleConsumer, error) {
	opts := []kgo.Opt{
		kafka.WithTracer(nil), // do not record traces
		kgo.ConsumeTopics(cfg.PlanTopic),
		kafka.WithConsumeOldestOffset(),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kafka.WithMaxPollRecords(10),                   // use a low max poll, as it takes ~1s to process one record, and we should give it a chance to process rebalances.
		kgo.SessionTimeout(1 * time.Minute),            // session timeout is 1 minute
		kgo.HeartbeatInterval(15 * time.Second),        // increase heartbeat interval to 15 seconds, as processing a record is slow (~1s / record)
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // use a manual partitioner, as all the restore records have a partition set and we should keep the same partition
	}
	if cfg.BrokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv))
	} else {
		opts = append(opts, kgo.SeedBrokers(internal.SplitAndTrim(cfg.Brokers, ",")...))
	}

	return kafka.NewSimpleConsumer(opts...)
}
