package avro

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
)

func TestCodec(t *testing.T) {
	epoch1 := int32(1)
	epoch2 := int32(2)
	metadata1 := "metadata1"

	offsets := []*codec.ConsumerGroupOffset{
		{
			GroupID: "group1",
			Topics: []codec.TopicOffset{
				{
					Topic: "topic1",
					Partitions: []codec.PartitionOffset{
						{
							Partition:   0,
							Offset:      100,
							LeaderEpoch: &epoch1,
							Metadata:    &metadata1,
						},
						{
							Partition:   1,
							Offset:      200,
							LeaderEpoch: &epoch2,
							Metadata:    nil,
						},
					},
				},
			},
		},
		{
			GroupID: "group2",
			Topics: []codec.TopicOffset{
				{
					Topic: "topic2",
					Partitions: []codec.PartitionOffset{
						{
							Partition:   0,
							Offset:      500,
							LeaderEpoch: nil,
							Metadata:    nil,
						},
					},
				},
			},
		},
	}

	// 1. Encode
	var buf bytes.Buffer
	encFactory := &GroupEncoderFactory{}
	encoder, err := encFactory.New(&buf)
	require.NoError(t, err, "failed to create encoder")

	for _, o := range offsets {
		err := encoder.Encode(o)
		require.NoError(t, err, "failed to encode offset")
	}

	require.NoError(t, encoder.Close())

	// 2. Decode
	decFactory := &GroupDecoderFactory{}
	decoder, err := decFactory.New(&buf)
	require.NoError(t, err, "failed to create decoder")

	decodedOffsets := make([]*codec.ConsumerGroupOffset, 0, len(offsets))
	for decoder.HasNext() {
		o, err := decoder.Decode()
		require.NoError(t, err, "failed to decode offset")
		decodedOffsets = append(decodedOffsets, o)
	}

	// 3. Compare
	require.Len(t, decodedOffsets, len(offsets), "offsets count mismatch")

	for i, expected := range offsets {
		actual := decodedOffsets[i]
		assert.Equal(t, expected, actual, "offset %d mismatch", i)
	}
}
