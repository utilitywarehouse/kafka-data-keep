package testutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
)

func StartKafkaService(t *testing.T) (string, func()) {
	t.Helper()
	ctx := t.Context()
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

func WaitConsumerStart(t *testing.T, client *kadm.Client, groupID string) {
	t.Helper()
	ctx := t.Context()
	timeoutC := time.After(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("consumer group %s did not start in 10 seconds", groupID)
		case <-time.Tick(100 * time.Millisecond):
			dg, err := client.DescribeGroups(ctx, groupID)
			require.NoError(t, err)
			if dg[groupID].State == "Stable" {
				t.Logf("consumer group %s started consuming", groupID)
				return
			}
		}
	}
}

func WaitConsumeAll(t *testing.T, kadmClient *kadm.Client, topic string, group string) {
	t.Helper()
	ctx := t.Context()
	endOffsets, err := kadmClient.ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	totalOffsets := 0
	for _, off := range endOffsets[topic] {
		totalOffsets += int(off.Offset)
	}
	WaitForGroupOffsets(t, kadmClient, group, map[string]int{topic: totalOffsets})
}

func WaitForGroupOffsets(t *testing.T, client *kadm.Client, group string, expected map[string]int) {
	t.Helper()
	ctx := t.Context()

	timeoutC := time.After(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("consumer group %s did not reach expected offsets: %+v", group, expected)
			return
		case <-time.Tick(100 * time.Millisecond):
			if isGroupAt(t, client, group, expected) {
				return
			}
		}
	}
}

func isGroupAt(t *testing.T, client *kadm.Client, group string, expected map[string]int) bool {
	t.Helper()
	ctx := t.Context()
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

func WaitForRecords(t *testing.T, topic string, kafkaBrokers string, expectedCount int) ([]*kgo.Record, error) {
	t.Helper()

	ctx := t.Context()

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
			return nil, nil
		default:
			if len(records) >= expectedCount {
				return records, nil
			}

			fetches := client.PollFetches(ctx)
			stopProcessing, err := kafka.HandleFetches(ctx, &fetches)
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

// ReadAll reads all records from the topic up to the high watermark at start.
func ReadAll(t *testing.T, topic string, kafkaBrokers string) ([]*kgo.Record, error) {
	t.Helper()
	ctx := t.Context()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	require.NoError(t, err)
	defer client.Close()

	admClient := kadm.NewClient(client)
	endOffsets, err := admClient.ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	defer admClient.Close()

	topicEndOffsets := endOffsets[topic]
	// If no partitions, return empty
	if len(topicEndOffsets) == 0 {
		return []*kgo.Record{}, nil
	}

	var records []*kgo.Record
	completedPartitions := make(map[int32]bool)

	// Mark empty partitions as completed immediately
	for p, endVal := range topicEndOffsets {
		if endVal.Offset == 0 {
			completedPartitions[p] = true
		}
	}

	// Check if we are already done (empty topic)
	if len(completedPartitions) == len(topicEndOffsets) {
		return records, nil
	}

	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			t.Fatalf("timed out waiting for consumer to catch up. Records: %d. Completed Partitions: %d/%d",
				len(records), len(completedPartitions), len(topicEndOffsets))
			return nil, nil
		default:
			// Poll
			fetches := client.PollFetches(ctx)
			stopProcessing, err := kafka.HandleFetches(ctx, &fetches)
			if stopProcessing || err != nil {
				return nil, err
			}

			fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
				records = append(records, ftp.Records...)
				lastRecord := ftp.Records[len(ftp.Records)-1]

				completedPartitions[ftp.Partition] = lastRecord.Offset >= topicEndOffsets[ftp.Partition].Offset
			})
			// Check if all partitions are done
			if len(completedPartitions) == len(topicEndOffsets) {
				return records, nil
			}
		}
	}
}
