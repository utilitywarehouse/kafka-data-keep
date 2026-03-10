package restore

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec/avro"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
)

// AppConfig holds the configuration for the consumer groups restore command.
type AppConfig struct {
	kafka.Config
	internal.OpsConfig
	S3Bucket            string
	S3Region            string
	S3Endpoint          string
	S3Location          string
	RestoreGroupsPrefix string
	RestoreTopicsPrefix string
	IncludeRegexes      string
	ExcludeRegexes      string
	LoopInterval        time.Duration
}

// Run executes the consumer groups restore process.
func Run(ctx context.Context, cfg AppConfig) error {
	if cfg.S3Bucket == "" {
		return fmt.Errorf("s3-bucket must be provided")
	}
	if cfg.S3Location == "" {
		return fmt.Errorf("s3-location must be provided")
	}

	includeRegexes, err := internal.CompileRegexes(cfg.IncludeRegexes)
	if err != nil {
		return fmt.Errorf("compiling include regexes: %w", err)
	}

	excludeRegexes, err := internal.CompileRegexes(cfg.ExcludeRegexes)
	if err != nil {
		return fmt.Errorf("compiling exclude regexes: %w", err)
	}

	offsets, err := downloadAndDecode(ctx, cfg, includeRegexes, excludeRegexes)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Decoded consumer group offsets from S3", "count", len(offsets))

	client, err := initKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}
	defer client.Close()

	restorer, err := NewRestorer(client, cfg.RestoreGroupsPrefix, cfg.RestoreTopicsPrefix)
	if err != nil {
		return fmt.Errorf("creating restorer: %w", err)
	}
	defer restorer.Close()
	return restorer.Restore(ctx, offsets, cfg.LoopInterval)
}

func downloadAndDecode(ctx context.Context, cfg AppConfig, includeRegexes, excludeRegexes []*regexp.Regexp) ([]codec.ConsumerGroupOffset, error) {
	s3Client, err := initS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}

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
		cgo, err := decoder.Decode()
		if err != nil {
			return nil, fmt.Errorf("decoding consumer group offset: %w", err)
		}
		if shouldProcess(ctx, cgo, includeRegexes, excludeRegexes) {
			offsets = append(offsets, *cgo)
		}
	}
	if decoder.Error() != nil {
		return offsets, fmt.Errorf("decoder error: %w", decoder.Error())
	}

	return offsets, nil
}

func shouldProcess(ctx context.Context, cgo *codec.ConsumerGroupOffset, includeRegexes []*regexp.Regexp, excludeRegexes []*regexp.Regexp) bool {
	if !internal.MatchesAny(cgo.GroupID, includeRegexes) {
		slog.InfoContext(ctx, "Skipping consumer group as it doesn't match the inclusion criteria", "group", cgo.GroupID)
		return false
	}

	if internal.MatchesAny(cgo.GroupID, excludeRegexes) {
		slog.InfoContext(ctx, "Skipping consumer group as it matches the exclusion criteria", "group", cgo.GroupID)
		return false
	}

	if isEmpty(cgo) {
		slog.InfoContext(ctx, "Skipping consumer group as it is empty", "group", cgo.GroupID)
		return false
	}
	slog.InfoContext(ctx, "Including consumer group", "group", cgo.GroupID)
	return true
}

func isEmpty(cgo *codec.ConsumerGroupOffset) bool {
	// check if there are any partitions saved in the consumer group
	for _, to := range cgo.Topics {
		if len(to.Partitions) > 0 {
			return false
		}
	}
	return true
}

func initS3Client(ctx context.Context, cfg AppConfig) (*s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.S3Region))
	if err != nil {
		return nil, fmt.Errorf("failed loading aws config: %w", err)
	}

	var s3ClientOpts []func(*s3.Options)
	if cfg.S3Endpoint != "" {
		s3ClientOpts = append(s3ClientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3ClientOpts...)
	return s3Client, nil
}

func initKafkaClient(ctx context.Context, cfg AppConfig) (*kgo.Client, error) {
	opts, err := kafka.BaseOpts(cfg.Config)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return client, err
	}
	if err := client.Ping(ctx); err != nil {
		return client, fmt.Errorf("failed pinging kafka: %w", err)
	}
	return client, nil
}
