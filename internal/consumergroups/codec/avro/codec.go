package avro

import (
	_ "embed"
	"fmt"
	"io"

	"github.com/hamba/avro/v2/ocf"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
)

//go:embed schema.json
var schema string

type GroupEncoderFactory struct{}

func (f *GroupEncoderFactory) New(w io.Writer) (codec.GroupEncoder, error) {
	encoder, err := ocf.NewEncoder(schema, w, ocf.WithCodec(ocf.ZStandard))
	if err != nil {
		return nil, fmt.Errorf("failed creating ocf encoder: %w", err)
	}

	return &groupEncoder{avroEncoder: *encoder}, nil
}

type groupEncoder struct {
	avroEncoder ocf.Encoder
}

func (e *groupEncoder) Encode(o *codec.ConsumerGroupOffset) error {
	if err := e.avroEncoder.Encode(o); err != nil {
		return fmt.Errorf("failed encoding consumer group offset: %w", err)
	}
	return nil
}

func (e *groupEncoder) Flush() error {
	return e.avroEncoder.Flush()
}

func (e *groupEncoder) Close() error {
	return e.avroEncoder.Close()
}

type GroupDecoderFactory struct{}

func (f *GroupDecoderFactory) New(r io.Reader) (codec.GroupDecoder, error) {
	decoder, err := ocf.NewDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("failed creating ocf decoder: %w", err)
	}
	return &groupDecoder{avroDecoder: *decoder}, nil
}

type groupDecoder struct {
	avroDecoder ocf.Decoder
}

func (d *groupDecoder) HasNext() bool {
	return d.avroDecoder.HasNext()
}

func (d *groupDecoder) Error() error {
	return d.avroDecoder.Error()
}

func (d *groupDecoder) Decode() (*codec.ConsumerGroupOffset, error) {
	var o codec.ConsumerGroupOffset
	if err := d.avroDecoder.Decode(&o); err != nil {
		return nil, fmt.Errorf("failed decoding consumer group offset: %w", err)
	}

	return &o, nil
}
