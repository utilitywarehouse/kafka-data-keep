package avro

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCodec(t *testing.T) {
	// 1. Setup records
	records := []*kgo.Record{
		{
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Topic:     "topic1",
			Partition: 0,
			Offset:    1,
			Timestamp: time.Now().Truncate(time.Millisecond), // Avro stores timestamp in millis
			Headers: []kgo.RecordHeader{
				{Key: "header1", Value: []byte("hvalue1")},
			},
		},
		{
			Key:       []byte("key2"),
			Value:     []byte("value2"),
			Topic:     "topic1",
			Partition: 0,
			Offset:    2,
			Timestamp: time.Now().Truncate(time.Millisecond),
			Headers: []kgo.RecordHeader{
				{Key: "header2", Value: []byte("hvalue2")},
				{Key: "header3", Value: []byte("hvalue3")},
			},
		},
		{
			Key:       []byte("key3"),
			Value:     []byte("value3"),
			Topic:     "topic2",
			Partition: 1,
			Offset:    10,
			Timestamp: time.Now().Truncate(time.Millisecond),
			Headers:   nil, // No headers
		},
	}

	// 2. Encode
	var buf bytes.Buffer
	encFactory := &RecordEncoderFactory{}
	encoder, err := encFactory.New(&buf)
	require.NoError(t, err, "failed to create encoder")

	for _, r := range records {
		err := encoder.Encode(r)
		require.NoError(t, err, "failed to encode record")
	}

	require.NoError(t, encoder.Close())

	// 3. Decode
	decFactory := &RecordDecoderFactory{}
	decoder, err := decFactory.New(&buf)
	require.NoError(t, err, "failed to create decoder")

	var decodedRecords []*kgo.Record
	for decoder.HasNext() {
		rec, err := decoder.Decode()
		require.NoError(t, err, "failed to decode record")
		decodedRecords = append(decodedRecords, rec)
	}

	// 4. Compare
	require.Equal(t, len(records), len(decodedRecords), "records count mismatch")

	for i, expected := range records {
		actual := decodedRecords[i]
		// Can not use comparing the records directly due to the headers.
		// Compare fields one by one
		assert.Equal(t, expected.Key, actual.Key, "record %d: key mismatch", i)
		assert.Equal(t, expected.Value, actual.Value, "record %d: value mismatch", i)
		assert.Equal(t, expected.Topic, actual.Topic, "record %d: topic mismatch", i)
		assert.Equal(t, expected.Partition, actual.Partition, "record %d: partition mismatch", i)
		assert.Equal(t, expected.Offset, actual.Offset, "record %d: offset mismatch", i)
		assert.Equal(t, expected.Timestamp, actual.Timestamp, "record %d: timestamp mismatch", i)

		if len(expected.Headers) == 0 {
			assert.Empty(t, actual.Headers, "record %d: expected empty headers", i)
		} else {
			// for comparing, convert both to map
			expMap := headersToMap(expected.Headers)
			actMap := headersToMap(actual.Headers)
			assert.Equal(t, expMap, actMap, "record %d: headers mismatch", i)
		}
	}
}

func headersToMap(headers []kgo.RecordHeader) map[string]string {
	m := make(map[string]string)
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
