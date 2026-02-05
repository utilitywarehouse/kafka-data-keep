package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReadLatest consumes the last message (tip) for the specified partitions in the specified topic. If no partition is specified, all are read.
func ReadLatest(ctx context.Context, seedBrokers []string, tls *tls.Config, topic string, onlyPartitions ...int32) (map[int32]*kgo.Record, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(seedBrokers...), kgo.DialTLSConfig(tls))
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	defer client.Close()
	endOffsets, startOffsets, err := getTopicOffsets(ctx, client, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic offsets for topic %s: %w", topic, err)
	}

	if len(endOffsets) == 0 {
		slog.DebugContext(ctx, "Topic not found or has no partitions.", "topic", topic)
		return nil, nil
	}

	tipOffsets := computePartitionsLatest(ctx, endOffsets, startOffsets, onlyPartitions)

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: tipOffsets,
	})

	return consumeLatest(ctx, client, len(tipOffsets))
}

func getTopicOffsets(ctx context.Context, client *kgo.Client, topic string) (map[int32]kadm.ListedOffset, map[int32]kadm.ListedOffset, error) {
	adm := kadm.NewClient(client)

	slog.DebugContext(ctx, "Fetching metadata for topic...", "topic", topic)
	listedOffsets, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list offsets: %w", err)
	}

	listedStartOffsets, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list start offsets: %w", err)
	}

	return listedOffsets[topic], listedStartOffsets[topic], nil
}

func computePartitionsLatest(ctx context.Context, endOffsets, startOffsets map[int32]kadm.ListedOffset, onlyPartitions []int32) map[int32]kgo.Offset {
	partitionsLatest := make(map[int32]kgo.Offset, len(endOffsets))

	for partition, endOffsetInfo := range endOffsets {
		if excludePartition(onlyPartitions, partition) {
			continue
		}
		lowWatermark := int64(0)
		if startInfo, ok := startOffsets[partition]; ok {
			lowWatermark = startInfo.Offset
		}

		latestOffset := endOffsetInfo.Offset - 1

		// If the tip is below the low watermark (deleted/expired), we can't consume it.
		// So we don't add it to the consumeMap.
		if latestOffset < lowWatermark {
			slog.DebugContext(ctx, "Partition is empty, skipping.", "partition", partition, "tip", latestOffset, "lowWatermark", lowWatermark)
			continue
		}

		partitionsLatest[partition] = kgo.NewOffset().At(latestOffset)
	}

	return partitionsLatest
}

func excludePartition(onlyPartitions []int32, partition int32) bool {
	return len(onlyPartitions) > 0 && !slices.Contains(onlyPartitions, partition)
}

func consumeLatest(ctx context.Context, client *kgo.Client, expectedCount int) (map[int32]*kgo.Record, error) {
	if expectedCount == 0 {
		return nil, nil
	}

	results := make(map[int32]*kgo.Record, expectedCount)
	for {
		if len(results) == expectedCount {
			return results, nil
		}

		fetches := client.PollRecords(ctx, 1000)
		stopProcessing, err := HandleFetches(ctx, &fetches)
		if err != nil {
			return nil, fmt.Errorf("failed to poll records at tip: %w", err)
		}
		if stopProcessing {
			return nil, nil
		}

		fetches.EachRecord(func(r *kgo.Record) {
			// We only need one record per partition (the tip)
			if _, exists := results[r.Partition]; !exists {
				results[r.Partition] = r
			}
		})
	}
}
