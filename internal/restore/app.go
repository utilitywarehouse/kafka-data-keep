package restore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
	"io"
	"log/slog"
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
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer planConsumer.Close()

	slog.InfoContext(ctx, "Starting restore application...")
	return runS3Restore(ctx, planConsumer, s3Client, cfg)
}

func runS3Restore(ctx context.Context, consumer *kafka.SimpleConsumer, s3Client *s3.Client, cfg AppConfig) error {
	return consumer.Consume(ctx, func(ctx context.Context, rec *kgo.Record) error {
		key := string(rec.Value)
		err := restoreFile(ctx, key, s3Client, consumer.Client, cfg)
		if err != nil {
			return fmt.Errorf("failed restoring file %s: %w", key, err)
		}

		return err
	})
}

func restoreFile(ctx context.Context, key string, s3Client *s3.Client, kafkaClient *kafka.Client, cfg AppConfig) error {
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}

	recs, err := decodeAvroFile(ctx, getResp.Body, cfg.RestoreTopicPrefix)
	if err != nil {
		return fmt.Errorf("failed to decode Avro file: %w", err)
	}
	// use background context as we want to push the records through without stopping if the context was canceled.
	res := kafkaClient.ProduceSync(context.Background(), recs...)
	if res.FirstErr() != nil {
		return fmt.Errorf("failed to produce records: %w", res.FirstErr())
	}
	slog.InfoContext(ctx, "Restored file", "key", key)
	return nil
}

func initKafkaConsumer(cfg AppConfig) (*kafka.SimpleConsumer, error) {
	opts := []kgo.Opt{
		kafka.WithTracer(nil), // do not record traces
		kgo.ConsumeTopics(cfg.PlanTopic),
		kafka.WithConsumeOldestOffset(),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kafka.WithMaxPollRecords(10), // use a low max poll, as it takes ~1s to process one record
	}
	if cfg.BrokersDNSSrv != "" {
		opts = append(opts, kafka.SeedBrokersFromDNS(cfg.BrokersDNSSrv))
	} else {
		opts = append(opts, kgo.SeedBrokers(cfg.Brokers))
	}

	return kafka.NewSimpleConsumer(opts...)
}

func decodeAvroFile(ctx context.Context, r io.ReadCloser, topicPrefix string) ([]*kgo.Record, error) {
	defer func() {
		if err := r.Close(); err != nil {
			slog.ErrorContext(ctx, "Failed to close Avro file reader: %v", err)
		}
	}()
	decFactory := &avro.RecordDecoderFactory{}
	decoder, err := decFactory.New(r)
	if err != nil {
		return nil, fmt.Errorf("failed creating Avro decoder: %w", err)
	}

	records := make([]*kgo.Record, 0, 1000)
	for decoder.HasNext() {
		rec, err := decoder.Decode()
		if err != nil {
			return nil, fmt.Errorf("failed decoding Avro record: %w", err)
		}
		rec.Topic = topicPrefix + rec.Topic
		records = append(records, rec)
	}

	if decoder.Error() != nil {
		return nil, fmt.Errorf("failed decoding Avro file: %w", decoder.Error())
	}

	return records, nil
}
