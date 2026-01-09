package restore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	kafka2 "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	"github.com/utilitywarehouse/kafka-data-keep/internal/planrestore"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

type kafkaS3Restorer struct {
	s3Client *s3.Client
	consumer *kafka.SimpleConsumer
	cfg      AppConfig

	// map containing the original offset of the last restored message per partition
	resumedOffsetsPerPartition map[string]int64
}

func (r *kafkaS3Restorer) Run(ctx context.Context) error {
	r.resumedOffsetsPerPartition = make(map[string]int64)
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

	topic, partition, err := planrestore.TopicPartitionFromFileName(key)
	if err != nil {
		return fmt.Errorf("failed to extract topic from file name: %w", err)
	}

	resumeOffset, err := r.getResumeOffset(ctx, topic, partition)
	if err != nil {
		return fmt.Errorf("failed determining resume offset: %w", err)
	}

	recs, err := recordsInFile(ctx, getResp.Body, r.cfg.RestoreTopicPrefix, resumeOffset)
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

func (r *kafkaS3Restorer) getResumeOffset(ctx context.Context, topic string, partition string) (int64, error) {
	p, err := strconv.ParseInt(partition, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse partition number: %w", err)
	}
	partitionInt := int32(p)

	resumeOffsetKey := fmt.Sprintf("%s/%s", topic, partition)
	resumeOffset, exists := r.resumedOffsetsPerPartition[resumeOffsetKey]
	if exists {
		return resumeOffset, nil
	}

	seedBrokers := r.consumer.OptValue(kgo.SeedBrokers).([]string) //nolint:errcheck
	lastRecord, err := kafka2.ReadLatest(ctx, seedBrokers, r.cfg.PlanTopic, partitionInt)
	if err != nil {
		return -1, fmt.Errorf("failed to read latest record for topic %s partition %d: %w", topic, partitionInt, err)
	}

	if lastRecord == nil {
		r.resumedOffsetsPerPartition[resumeOffsetKey] = -1
		return -1, nil
	}

	resumeOffset, err = getOriginalOffsetFromHeader(lastRecord[partitionInt])
	if err != nil {
		return -1, fmt.Errorf("failed to get original offset from header: %w", err)
	}

	r.resumedOffsetsPerPartition[resumeOffsetKey] = resumeOffset
	return resumeOffset, nil
}

func getOriginalOffsetFromHeader(rec *kgo.Record) (int64, error) {
	for i := range rec.Headers {
		if rec.Headers[i].Key == originalOffsetHeader {
			offset, err := strconv.ParseInt(string(rec.Headers[i].Value), 10, 64)
			if err != nil {
				return -1, fmt.Errorf("failed parsing original offset header value: %w", err)
			}
			return offset, nil
		}
	}

	return -1, nil
}

const originalOffsetHeader = "original_offset"

func recordsInFile(ctx context.Context, r io.ReadCloser, topicPrefix string, resumeOffset int64) ([]*kgo.Record, error) {
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
		// skip records that were already restored
		if rec.Offset <= resumeOffset {
			continue
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
