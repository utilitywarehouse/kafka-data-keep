package codec

import (
	"io"
)

// PartitionOffset represents offset information for a single partition
type PartitionOffset struct {
	Partition   int32   `avro:"partition"`
	Offset      int64   `avro:"offset"`
	LeaderEpoch *int32  `avro:"leader_epoch"`
	Metadata    *string `avro:"metadata"`
}

// TopicOffset represents offset information for all partitions in a topic
type TopicOffset struct {
	Topic      string            `avro:"topic"`
	Partitions []PartitionOffset `avro:"partitions"`
}

// ConsumerGroupOffset represents offset information for a consumer group across multiple topics
type ConsumerGroupOffset struct {
	GroupID string        `avro:"group_id"`
	Topics  []TopicOffset `avro:"topics"`
}

// GroupEncoder encodes consumer group offsets to binary format
type GroupEncoder interface {
	io.Closer
	Encode(o *ConsumerGroupOffset) error
	Flush() error
}

// GroupEncoderFactory creates a new GroupEncoder
type GroupEncoderFactory interface {
	New(w io.Writer) (GroupEncoder, error)
}

// GroupDecoder decodes consumer group offsets from binary format
type GroupDecoder interface {
	Decode() (*ConsumerGroupOffset, error)
	HasNext() bool
	Error() error
}

// GroupDecoderFactory creates a new GroupDecoder
type GroupDecoderFactory interface {
	New(r io.Reader) (GroupDecoder, error)
}
