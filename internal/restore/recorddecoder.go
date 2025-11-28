package restore

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
)

type RecordDecoder interface {
	Decode() (*kgo.Record, error)
	HasNext() bool
	Error() error
}

type RecordDecoderFactory interface {
	New(r io.Reader) (RecordDecoder, error)
}
