package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReadLatest consumes the last message (tip) for each partition in the specified topic.
func ReadLatest(ctx context.Context, client *kgo.Client, topic string) (map[int32]*kgo.Record, error) {
	endOffsets, startOffsets, err := getTopicOffsets(ctx, client, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic offsets for topic %s: %w", topic, err)
	}

	if len(endOffsets) == 0 {
		slog.DebugContext(ctx, "Topic not found or has no partitions.", "topic", topic)
		return nil, nil
	}

	tipOffsets := computePartitionsLatest(ctx, endOffsets, startOffsets)

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: tipOffsets,
	})

	pList := make([]int32, 0, len(tipOffsets))
	for p := range tipOffsets {
		pList = append(pList, p)
	}

	defer client.RemoveConsumePartitions(map[string][]int32{
		topic: pList,
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

func computePartitionsLatest(ctx context.Context, endOffsets, startOffsets map[int32]kadm.ListedOffset) map[int32]kgo.Offset {
	partitionsLatest := make(map[int32]kgo.Offset, len(endOffsets))

	for partition, offsetInfo := range endOffsets {
		lowWatermark := int64(0)
		if startInfo, ok := startOffsets[partition]; ok {
			lowWatermark = startInfo.Offset
		}

		startOffset := offsetInfo.Offset - 1

		// If the tip is below the low watermark (deleted/expired), we can't consume it.
		// So we don't add it to the consumeMap.
		if startOffset < lowWatermark {
			slog.DebugContext(ctx, "Partition is empty, skipping.", "partition", partition, "tip", startOffset, "lowWatermark", lowWatermark)
			continue
		}

		partitionsLatest[partition] = kgo.NewOffset().At(startOffset)
	}

	return partitionsLatest
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
		err, stopProcessing := HandleFetches(ctx, &fetches)
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
