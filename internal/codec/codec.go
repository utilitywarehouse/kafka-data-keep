package codec

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
)

// RecordEncoder encodes kafka records to binary format
type RecordEncoder interface {
	io.Closer
	Encode(r *kgo.Record) error
	Flush() error
}

type RecordEncoderFactory interface {
	New(w io.Writer) (RecordEncoder, error)
}

// RecordDecoder decodes kafka records from binary format
type RecordDecoder interface {
	Decode() (*kgo.Record, error)
	HasNext() bool
	Error() error
}

type RecordDecoderFactory interface {
	New(r io.Reader) (RecordDecoder, error)
}
