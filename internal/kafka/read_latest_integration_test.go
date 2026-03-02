package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReadLastRecords(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := t.Context()

	// 1. Start Redpanda container (Shared)
	container, err := redpanda.Run(ctx, "docker.io/redpandadata/redpanda:v23.3.10")
	require.NoError(t, err)
	t.Cleanup(func() {
		// use the background context to terminate, as the test context is cancelled already at this point
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	seedBroker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(seedBroker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)

	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	reader, err := NewLatestReader([]string{seedBroker}, nil)
	require.NoError(t, err)
	t.Cleanup(reader.Close)

	t.Run("FullConsumption", func(t *testing.T) {
		t.Parallel()

		topic := "test-topic-full"
		partitions := int32(15)
		_, err = kadmClient.CreateTopics(ctx, partitions, 1, nil, topic)
		require.NoError(t, err)

		// 3. Produce messages
		// Produce 10 messages to each of the 15 partitions
		msgsPerPartition := 10
		// We expect to consume only the last message of each partition
		for p := range partitions {
			for i := range msgsPerPartition {
				record := &kgo.Record{
					Topic:     topic,
					Partition: p,
					Value:     fmt.Appendf(nil, "p%d-msg-%d", p, i),
				}
				result := adminClient.ProduceSync(ctx, record)
				require.NoError(t, result.FirstErr())
			}
		}

		records, err := reader.Read(ctx, topic)
		require.NoError(t, err)

		// 5. Validation
		assert.Len(t, records, int(partitions), "Should have consumed exactly 1 message per partition")
		for p := range partitions {
			r, ok := records[p]
			assert.True(t, ok, "Partition %d should have 1 consumed msg", p)
			expectedVal := fmt.Sprintf("p%d-msg-%d", p, msgsPerPartition-1)
			assert.Equal(t, expectedVal, string(r.Value), "Should have consumed the last message for partition %d", p)
		}

		//	consume a single partition
		partition := int32(3)
		records, err = reader.Read(ctx, topic, partition)
		require.NoError(t, err)
		assert.Len(t, records, 1, "Should have consumed only 1 partition")
		rec := records[partition]
		expectedVal := fmt.Sprintf("p%d-msg-%d", partition, msgsPerPartition-1)
		assert.Equal(t, expectedVal, string(rec.Value), "Should have consumed the last message for partition %d", partition)
	})

	t.Run("WithDeletedRecords", func(t *testing.T) {
		t.Parallel()

		topic := "test-topic-deleted"
		partitions := int32(2)
		_, err := kadmClient.CreateTopics(ctx, partitions, 1, nil, topic)
		require.NoError(t, err)

		// 2. Produce 10 messages (Offsets 0-9) to EACH partition
		for p := range partitions {
			for i := range 10 {
				record := &kgo.Record{
					Topic:     topic,
					Partition: p,
					Value:     fmt.Appendf(nil, "p%d-msg-%d", p, i),
				}
				result := adminClient.ProduceSync(ctx, record)
				require.NoError(t, result.FirstErr())
			}
		}

		// 3. Setup Deletions
		delMap := make(kadm.Offsets)

		// Partition 0: Partial Deletion (0-4 deleted, 5-9 remain)
		// Tip should be offset 9.
		delMap.Add(kadm.Offset{Topic: topic, Partition: 0, At: 5})

		// Partition 1: Full Deletion (0-9 deleted)
		// Tip should be empty (LWM becomes 10, HWM is 10).
		delMap.Add(kadm.Offset{Topic: topic, Partition: 1, At: 10})

		_, err = kadmClient.DeleteRecords(ctx, delMap)
		require.NoError(t, err)

		records, err := reader.Read(ctx, topic)
		require.NoError(t, err)

		// Verification
		// Should only have 1 record from Partition 0.
		// Partition 1 should be completely skipped.
		assert.Len(t, records, 1, "Should consume exactly 1 message (partition 0 only)")

		// Check Partition 0
		r0, ok := records[0]
		assert.True(t, ok, "Should have record for partition 0")
		assert.Equal(t, int64(9), r0.Offset, "Partition 0 should be at offset 9")
		assert.Equal(t, "p0-msg-9", string(r0.Value))
	})

	t.Run("EmptyTopic", func(t *testing.T) {
		t.Parallel()

		topic := "test-topic-empty"
		// Create topic
		_, err := kadmClient.CreateTopics(ctx, int32(5), 1, nil, topic)
		require.NoError(t, err)

		// Consume all partitions
		records, err := reader.Read(ctx, topic)
		require.NoError(t, err)
		assert.Empty(t, records, "Should return empty map for empty topic")

		// Consume a single partition
		records, err = reader.Read(ctx, topic, int32(3))
		require.NoError(t, err)
		assert.Empty(t, records, "Should return empty map for empty topic when reading a single partition")
	})
}
