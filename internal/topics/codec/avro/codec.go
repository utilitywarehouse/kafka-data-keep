package avro

import (
	_ "embed"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/codec"
)

//go:embed schema.json
var schema string

type kafkaRecord struct {
	Key       []byte            `avro:"key"`
	Value     []byte            `avro:"value"`
	Headers   map[string][]byte `avro:"headers"`
	Topic     string            `avro:"topic"`
	Partition int32             `avro:"partition"`
	Timestamp int64             `avro:"timestamp"`
	Offset    int64             `avro:"offset"`
}

type RecordEncoderFactory struct{}

func (f *RecordEncoderFactory) New(w io.Writer) (codec.RecordEncoder, error) {
	encoder, err := ocf.NewEncoder(schema, w, ocf.WithCodec(ocf.ZStandard))
	if err != nil {
		return nil, fmt.Errorf("failed creating ocf encoder: %w", err)
	}

	return &recordEncoder{avroEncoder: *encoder}, nil
}

type recordEncoder struct {
	avroEncoder ocf.Encoder
}

func (e *recordEncoder) Encode(record *kgo.Record) error {
	if err := e.avroEncoder.Encode(toAvro(record)); err != nil {
		return fmt.Errorf("failed encoding kafka record: %w", err)
	}
	return nil
}

func (e *recordEncoder) Flush() error {
	return e.avroEncoder.Flush()
}

func toAvro(r *kgo.Record) kafkaRecord {
	headers := make(map[string][]byte, len(r.Headers))
	for _, h := range r.Headers {
		if _, exists := headers[h.Key]; exists {
			slog.Warn("duplicate header key detected — only last value will be preserved in backup",
				"key", h.Key, "topic", r.Topic, "offset", r.Offset)
		}
		headers[h.Key] = h.Value
	}

	return kafkaRecord{
		Key:       r.Key,
		Value:     r.Value,
		Headers:   headers,
		Topic:     r.Topic,
		Partition: r.Partition,
		Timestamp: r.Timestamp.UnixMilli(),
		Offset:    r.Offset,
	}
}

func (e *recordEncoder) Close() error {
	return e.avroEncoder.Close()
}

type RecordDecoderFactory struct{}

func (f *RecordDecoderFactory) New(r io.Reader) (codec.RecordDecoder, error) {
	decoder, err := ocf.NewDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("failed creating ocf decoder: %w", err)
	}
	return &recordDecoder{avroDecoder: *decoder}, nil
}

type recordDecoder struct {
	avroDecoder ocf.Decoder
}

func (d *recordDecoder) HasNext() bool {
	return d.avroDecoder.HasNext()
}

func (d *recordDecoder) Error() error {
	return d.avroDecoder.Error()
}

func (d *recordDecoder) Decode() (*kgo.Record, error) {
	var avroRec kafkaRecord
	if err := d.avroDecoder.Decode(&avroRec); err != nil {
		return nil, fmt.Errorf("failed decoding kafka record: %w", err)
	}

	return toKgo(avroRec), nil
}

func toKgo(avroRec kafkaRecord) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(avroRec.Headers))
	for k, v := range avroRec.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: v})
	}

	return &kgo.Record{
		Key:       avroRec.Key,
		Value:     avroRec.Value,
		Headers:   headers,
		Topic:     avroRec.Topic,
		Partition: avroRec.Partition,
		Timestamp: time.UnixMilli(avroRec.Timestamp),
		Offset:    avroRec.Offset,
	}
}
