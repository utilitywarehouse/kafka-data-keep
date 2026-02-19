package restore

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
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec/avro"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

// AppConfig holds the configuration for the consumer groups restore command.
type AppConfig struct {
	Brokers        string
	BrokersDNSSrv  string
	S3Bucket       string
	S3Region       string
	S3Endpoint     string
	S3Location     string
	RestorePrefix  string
	IncludeRegexes string
	LoopInterval   time.Duration
}

// Run executes the consumer groups restore process.
func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3Bucket == "" {
		return fmt.Errorf("s3-bucket must be provided")
	}
	if cfg.S3Location == "" {
		return fmt.Errorf("s3-location must be provided")
	}

	regexes, err := internal.CompileRegexes(cfg.IncludeRegexes)
	if err != nil {
		return fmt.Errorf("compiling include regexes: %w", err)
	}

	offsets, err := downloadAndDecode(ctx, cfg)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Decoded consumer group offsets from S3", "count", len(offsets))

	client, seedBrokers, err := initKafkaClient(cfg)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}
	defer client.Close()

	kadmClient := kadm.NewClient(client.Client)
	defer kadmClient.Close()

	restorer := NewRestorer(kadmClient, seedBrokers, nil, cfg.RestorePrefix)
	return restorer.Restore(ctx, offsets, regexes, cfg.LoopInterval)
}

func downloadAndDecode(ctx context.Context, cfg AppConfig) ([]codec.ConsumerGroupOffset, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.S3Region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var s3ClientOpts []func(*s3.Options)
	if cfg.S3Endpoint != "" {
		s3ClientOpts = append(s3ClientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3ClientOpts...)

	getObj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(cfg.S3Location),
	})
	if err != nil {
		return nil, fmt.Errorf("downloading backup file from S3: %w", err)
	}
	defer func() {
		_ = getObj.Body.Close()
	}()

	decFactory := &avro.GroupDecoderFactory{}
	decoder, err := decFactory.New(getObj.Body)
	if err != nil {
		return nil, fmt.Errorf("creating avro decoder: %w", err)
	}

	var offsets []codec.ConsumerGroupOffset
	for decoder.HasNext() {
		o, err := decoder.Decode()
		if err != nil {
			return nil, fmt.Errorf("decoding consumer group offset: %w", err)
		}
		offsets = append(offsets, *o)
	}
	if decoder.Error() != nil {
		return nil, fmt.Errorf("decoder error: %w", decoder.Error())
	}

	return offsets, nil
}

func initKafkaClient(cfg AppConfig) (*kafka.Client, []string, error) {
	var connectOpt kgo.Opt
	var seedBrokers []string

	if cfg.BrokersDNSSrv != "" {
		connectOpt = kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv)
	} else if cfg.Brokers != "" {
		seedBrokers = strings.Split(cfg.Brokers, ",")
		connectOpt = kgo.SeedBrokers(seedBrokers...)
	}

	client, err := kafka.NewClient(connectOpt)
	if err != nil {
		return nil, nil, err
	}
	return client, seedBrokers, nil
}
