package restore

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

type kafkaS3Restorer struct {
	s3Client *s3.Client
	consumer *kafka.SimpleConsumer
	cfg      AppConfig
}

func (r *kafkaS3Restorer) Run(ctx context.Context) error {
	return r.consumer.Consume(ctx, func(ctx context.Context, rec *kgo.Record) error {
		key := string(rec.Value)
		err := r.restoreFile(ctx, key)
		if err != nil {
			return fmt.Errorf("failed restoring file %s: %w", key, err)
		}

		return err
	})
}

func (r *kafkaS3Restorer) restoreFile(ctx context.Context, key string) error {
	getResp, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.cfg.S3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}

	recs, err := decodeAvroFile(ctx, getResp.Body, r.cfg.RestoreTopicPrefix)
	if err != nil {
		return fmt.Errorf("failed to decode Avro file: %w", err)
	}
	//nolint:contextcheck //use background context as we want to push the records through without stopping if the context was canceled.
	res := r.consumer.ProduceSync(context.Background(), recs...)
	if res.FirstErr() != nil {
		return fmt.Errorf("failed to produce records: %w", res.FirstErr())
	}
	slog.InfoContext(ctx, "Restored file", "key", key, "records", len(recs), "last_offset", recs[len(recs)-1].Offset)
	return nil
}

const originalOffsetHeader = "original_offset"

func decodeAvroFile(ctx context.Context, r io.ReadCloser, topicPrefix string) ([]*kgo.Record, error) {
	defer func() {
		if err := r.Close(); err != nil {
			slog.ErrorContext(ctx, "Failed closing Avro file reader", "error", err)
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
		// set the original offset header -> we're using for dedup and consumer group restoring
		setOriginalOffsetHeader(rec)
		records = append(records, rec)
	}

	if decoder.Error() != nil {
		return nil, fmt.Errorf("failed decoding Avro file: %w", decoder.Error())
	}

	return records, nil
}

func setOriginalOffsetHeader(rec *kgo.Record) {
	val := fmt.Appendf(nil, "%d", rec.Offset)
	// check if the header already exists from a previous restore
	for i := range rec.Headers {
		if rec.Headers[i].Key == originalOffsetHeader {
			rec.Headers[i].Value = val
			return
		}
	}

	rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: originalOffsetHeader, Value: val})
}
