package restore

import (
	"context"
	"crypto/tls"
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
	lastProcessedOffsetByPartition map[string]int64
}

func (r *kafkaS3Restorer) Run(ctx context.Context) error {
	r.lastProcessedOffsetByPartition = make(map[string]int64)
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
	//nolint:contextcheck // use background context as otherwise, if the context is canceled, it fails when decoding the file
	getResp, err := r.s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

	lastProcessedOffset, err := r.getLastProcessedOffset(ctx, topic, partition)
	if err != nil {
		return fmt.Errorf("failed determining resume offset: %w", err)
	}

	recs, err := r.recordsInFile(ctx, getResp.Body, lastProcessedOffset)
	if err != nil {
		return fmt.Errorf("failed to decode Avro file: %w", err)
	}

	if len(recs) == 0 {
		slog.InfoContext(ctx, "Restored file, but all records were skipped from it", "key", key, "last_processed_offset", lastProcessedOffset)
		return nil
	}

	//nolint:contextcheck //use background context as we want to push the records through without stopping if the context was canceled.
	res := r.consumer.ProduceSync(context.Background(), recs...)

	// update the last processed offset for this partition
	r.lastProcessedOffsetByPartition[partitionKey(topic, partition)] = recs[len(recs)-1].Offset

	if res.FirstErr() != nil {
		return fmt.Errorf("failed to produce records: %w", res.FirstErr())
	}

	slog.InfoContext(ctx, "Restored file", "key", key, "records", len(recs), "last_offset", recs[len(recs)-1].Offset, "last_processed_offset", lastProcessedOffset)
	return nil
}

func (r *kafkaS3Restorer) getLastProcessedOffset(ctx context.Context, topic string, partition string) (int64, error) {
	p, err := strconv.ParseInt(partition, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse partition number: %w", err)
	}
	partitionInt := int32(p)

	partitionKey := partitionKey(topic, partition)
	lastProcessedOffset, exists := r.lastProcessedOffsetByPartition[partitionKey]
	if exists {
		return lastProcessedOffset, nil
	}

	// if we didn't cache this yet, try to look it up from the destination topic from the last restored record, to cover resumes
	lastProcessedOffset, err = r.computeLastRestoredOffset(ctx, topic, partitionInt)
	if err != nil {
		return -1, err
	}

	r.lastProcessedOffsetByPartition[partitionKey] = lastProcessedOffset
	slog.InfoContext(ctx, "Computed last restored offset", "topic", topic, "partition", partition, "offset", lastProcessedOffset)
	return lastProcessedOffset, nil
}

func (r *kafkaS3Restorer) computeLastRestoredOffset(ctx context.Context, topic string, partitionInt int32) (int64, error) {
	seedBrokers := r.consumer.OptValue(kgo.SeedBrokers).([]string)    //nolint:errcheck // this would fail only if the franz-go lib changes, and we'll catch that in integration tests
	tlsConfig := r.consumer.OptValue(kgo.DialTLSConfig).(*tls.Config) //nolint:errcheck // this would fail only if the franz-go lib changes, and we'll catch that in integration tests

	lastRecord, err := kafka2.ReadLatest(ctx, seedBrokers, tlsConfig, r.restoreTopicName(topic), partitionInt)
	if err != nil {
		return -1, fmt.Errorf("failed to read latest record for topic %s partition %d: %w", topic, partitionInt, err)
	}

	if lastRecord == nil {
		slog.DebugContext(ctx, "compute last restored offset: no last record found", "topic", topic, "partition", partitionInt)
		return -1, nil
	}

	// if there is no last record, just start from -1
	lastRestoredOffset, err := getOriginalOffsetFromHeader(ctx, lastRecord[partitionInt])
	if err != nil {
		return -1, fmt.Errorf("failed to get original offset from header: %w", err)
	}
	return lastRestoredOffset, nil
}

func partitionKey(topic string, partition string) string {
	return fmt.Sprintf("%s/%s", topic, partition)
}

func getOriginalOffsetFromHeader(ctx context.Context, rec *kgo.Record) (int64, error) {
	for i := range rec.Headers {
		if rec.Headers[i].Key == originalOffsetHeader {
			offset, err := strconv.ParseInt(string(rec.Headers[i].Value), 10, 64)
			if err != nil {
				return -1, fmt.Errorf("failed parsing original offset header value: %w", err)
			}
			return offset, nil
		}
	}

	slog.DebugContext(ctx, "compute last restored offset: restore header not found in last record", "headers", rec.Headers, "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
	return -1, nil
}

const originalOffsetHeader = "original_offset"

func (r *kafkaS3Restorer) recordsInFile(ctx context.Context, is io.ReadCloser, lastProcessedOffset int64) ([]*kgo.Record, error) {
	defer func() {
		if err := is.Close(); err != nil {
			slog.ErrorContext(ctx, "Failed closing Avro file reader", "error", err)
		}
	}()
	decFactory := &avro.RecordDecoderFactory{}
	decoder, err := decFactory.New(is)
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
		if rec.Offset <= lastProcessedOffset {
			continue
		}
		rec.Topic = r.restoreTopicName(rec.Topic)
		// set the original offset header -> we're using for dedup and consumer group restoring
		setOriginalOffsetHeader(rec)
		records = append(records, rec)
	}

	if decoder.Error() != nil {
		return nil, fmt.Errorf("failed decoding Avro file: %w", decoder.Error())
	}

	return records, nil
}

func (r *kafkaS3Restorer) restoreTopicName(initialTopic string) string {
	return r.cfg.RestoreTopicPrefix + initialTopic
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
