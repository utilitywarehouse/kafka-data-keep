package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec/avro"
	topicsbackup "github.com/utilitywarehouse/kafka-data-keep/internal/topics/backup"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

type AppConfig struct {
	Brokers       string
	BrokersDNSSrv string
	S3Bucket      string
	S3Region      string
	S3Endpoint    string
	S3Location    string
	RunInterval   time.Duration
}

func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3Bucket == "" {
		return fmt.Errorf("bucket must be provided")
	}
	if cfg.S3Location == "" {
		return fmt.Errorf("s3-location must be provided")
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
	uploader := topicsbackup.NewUploader(s3Client, cfg.S3Bucket)

	client, err := initKafkaClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()

	kadmClient := kadm.NewClient(client)
	defer kadmClient.Close()

	encFactory := &avro.GroupEncoderFactory{}
	groupWriter := NewGroupWriter(kadmClient, uploader, encFactory, cfg)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := groupWriter.Backup(ctx); err != nil {
				slog.Error("failed to backup consumer groups", "error", err)
			}
		}
	}
}

func initKafkaClient(cfg AppConfig) (*kgo.Client, error) {
	opts := []kgo.Opt{}
	if cfg.BrokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv))
	} else if cfg.Brokers != "" {
		opts = append(opts, kgo.SeedBrokers(strings.Split(cfg.Brokers, ",")...))
	}

	return kgo.NewClient(opts...)
}
