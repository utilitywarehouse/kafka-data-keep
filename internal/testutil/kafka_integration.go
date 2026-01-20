package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	"time"
)

func StartKafkaService(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()
	redpandaContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v25.1.1")
	require.NoError(t, err)
	terminateFunc := func() {
		if err := redpandaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Redpanda container: %v", err)
		}
	}

	kafkaBrokers, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return kafkaBrokers, terminateFunc
}

func WaitConsumerStart(ctx context.Context, t *testing.T, client *kadm.Client, groupId string) {
	t.Helper()
	timeoutC := time.After(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("consumer group %s did not start in 10 seconds", groupId)
		case <-time.Tick(100 * time.Millisecond):
			dg, err := client.DescribeGroups(ctx, groupId)
			require.NoError(t, err)
			if dg[groupId].State == "Stable" {
				t.Logf("consumer group %s started consuming", groupId)
				return
			}
		}
	}
}

func WaitForGroupOffsets(t *testing.T, ctx context.Context, client *kadm.Client, group string, expected map[string]int) {
	t.Helper()
	timeoutC := time.After(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("consumer group %s did not reach expected offsets: %+v", group, expected)
			return
		case <-time.Tick(100 * time.Millisecond):
			if isGroupAt(t, ctx, client, group, expected) {
				return
			}
		}
	}
}

func isGroupAt(t *testing.T, ctx context.Context, client *kadm.Client, group string, expected map[string]int) bool {
	t.Helper()
	topics := make([]string, 0, len(expected))
	for t := range expected {
		topics = append(topics, t)
	}

	offsets, err := client.FetchOffsetsForTopics(ctx, group, topics...)
	require.NoError(t, err)

	for topic, exp := range expected {
		topicOffsets, hasTopicOffset := offsets[topic]
		if !hasTopicOffset {
			t.Logf("Topic %s: no offsets found", topic)
			return false
		}

		currentOffset := getOffsetForTopic(topicOffsets)
		t.Logf("Topic %s: current offset sum: %d, expected: %d", topic, currentOffset, exp)
		if currentOffset < exp {
			return false
		}
	}

	return true
}

func getOffsetForTopic(topicOffsets map[int32]kadm.OffsetResponse) int {
	// Sum offsets for all partitions of the topic
	currentOffsetSum := 0
	for _, pOff := range topicOffsets {
		if pOff.At > 0 {
			currentOffsetSum += int(pOff.At)
		}
	}
	return currentOffsetSum
}

func WaitForRecords(t *testing.T, ctx context.Context, topic string, kafkaBrokers string, expectedCount int) ([]*kgo.Record, error) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer client.Close()

	var records []*kgo.Record
	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for %d records, got %d", expectedCount, len(records))
		default:
			if len(records) >= expectedCount {
				return records, nil
			}

			fetches := client.PollFetches(ctx)
			err, stopProcessing := kafka.HandleFetches(ctx, &fetches)
			if stopProcessing || err != nil {
				return nil, err
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				records = append(records, iter.Next())
			}
		}
	}
}
