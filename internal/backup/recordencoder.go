package backup

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
)

// RecordEncoder encodes kafka records to binary format
type RecordEncoder interface {
	io.Closer
	Encode(r *kgo.Record) error
}

type RecordEncoderFactory interface {
	New(w io.Writer) (RecordEncoder, error)
}
