// Package restore implements consumer group offset restoration from S3 backups.
package restore

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	topicsrestore "github.com/utilitywarehouse/kafka-data-keep/internal/topics/restore"
)

// groupPartitionOffset holds a single consumer group's offset for one partition.
type groupPartitionOffset struct {
	GroupID   string
	Partition int32
	Offset    int64
}

// Restorer performs consumer group offset restoration.
type Restorer struct {
	kadmClient          *kadm.Client
	seedBrokers         []string
	tlsConfig           *tls.Config
	restoreGroupsPrefix string
	restoreTopicsPrefix string
}

// NewRestorer creates a new Restorer.
func NewRestorer(kadmClient *kadm.Client, seedBrokers []string, tlsConfig *tls.Config, restoreGroupsPrefix, restoreTopicsPrefix string) *Restorer {
	return &Restorer{
		kadmClient:          kadmClient,
		seedBrokers:         seedBrokers,
		tlsConfig:           tlsConfig,
		restoreGroupsPrefix: restoreGroupsPrefix,
		restoreTopicsPrefix: restoreTopicsPrefix,
	}
}

// Restore orchestrates the full consumer group offset restoration.
func (r *Restorer) Restore(ctx context.Context, offsets []codec.ConsumerGroupOffset, loopInterval time.Duration) error {
	remaining, err := r.filterAlreadyRestored(ctx, offsets)
	if err != nil {
		return fmt.Errorf("checking already restored groups: %w", err)
	}
	slog.InfoContext(ctx, "Filtered already restored groups", "remaining", len(remaining))

	grouped := groupByTopic(remaining)

	grouped, err = r.filterNonExistingTopics(ctx, grouped)
	if err != nil {
		return fmt.Errorf("filtering non existing topics: %w", err)
	}
	slog.InfoContext(ctx, "Filtered non existing topics", "topics", len(grouped))

	return r.runLoop(ctx, loopInterval, grouped)
}

// filterAlreadyRestored removes partitions that already have offsets committed
// in the Kafka cluster (using the prefixed group name), keeping only the ones not yet restored.
func (r *Restorer) filterAlreadyRestored(ctx context.Context, offsets []codec.ConsumerGroupOffset) ([]codec.ConsumerGroupOffset, error) {
	var result []codec.ConsumerGroupOffset
	for _, cg := range offsets {
		restoredGroupID := r.restoreGroupsPrefix + cg.GroupID
		fetched, err := r.kadmClient.FetchOffsets(ctx, restoredGroupID)
		if err != nil {
			return nil, fmt.Errorf("fetching offsets for group %s: %w", restoredGroupID, err)
		}

		filtered := filterTopicPartitions(ctx, cg, fetched)
		if len(filtered.Topics) > 0 {
			result = append(result, filtered)
		} else {
			slog.InfoContext(ctx, "Skipping fully restored group", "group", restoredGroupID)
		}
	}
	return result, nil
}

// filterTopicPartitions removes partitions that already have committed offsets from a consumer group.
func filterTopicPartitions(ctx context.Context, cg codec.ConsumerGroupOffset, fetched kadm.OffsetResponses) codec.ConsumerGroupOffset {
	filtered := codec.ConsumerGroupOffset{GroupID: cg.GroupID}
	for _, to := range cg.Topics {
		fetchedPartitions := fetched[to.Topic]
		var remaining []codec.PartitionOffset
		for _, po := range to.Partitions {
			if fetchedPartitions != nil {
				if resp, ok := fetchedPartitions[po.Partition]; ok && resp.At >= 0 {
					slog.DebugContext(ctx, "Skipping already restored partition",
						"group", cg.GroupID, "topic", to.Topic, "partition", po.Partition)
					continue
				}
			}
			remaining = append(remaining, po)
		}
		if len(remaining) > 0 {
			filtered.Topics = append(filtered.Topics, codec.TopicOffset{
				Topic:      to.Topic,
				Partitions: remaining,
			})
		}
	}
	return filtered
}

// groupByTopic transforms consumer group offsets into a map keyed by the restored topic name.
func groupByTopic(offsets []codec.ConsumerGroupOffset) map[string][]groupPartitionOffset {
	grouped := make(map[string][]groupPartitionOffset)
	for _, cg := range offsets {
		for _, to := range cg.Topics {
			for _, po := range to.Partitions {
				grouped[to.Topic] = append(grouped[to.Topic], groupPartitionOffset{
					GroupID:   cg.GroupID,
					Partition: po.Partition,
					Offset:    po.Offset,
				})
			}
		}
	}
	return grouped
}

// filterNonExistingTopics keeps only topics that exist in the Kafka cluster (with restore prefix).
func (r *Restorer) filterNonExistingTopics(ctx context.Context, grouped map[string][]groupPartitionOffset) (map[string][]groupPartitionOffset, error) {
	topics, err := r.kadmClient.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	existingTopics := make(map[string]bool, len(topics))
	for _, t := range topics {
		existingTopics[t.Topic] = true
	}

	result := make(map[string][]groupPartitionOffset)
	for topic, entries := range grouped {
		restoredTopic := r.restoreTopicsPrefix + topic
		if existingTopics[restoredTopic] {
			result[topic] = entries
		} else {
			slog.InfoContext(ctx, "Skipping topic not found in cluster", "topic", restoredTopic)
		}
	}
	return result, nil
}

// runLoop runs the restore process on a ticker until all offsets are resolved or context is cancelled.
func (r *Restorer) runLoop(ctx context.Context, interval time.Duration, grouped map[string][]groupPartitionOffset) error {
	if len(grouped) == 0 {
		slog.InfoContext(ctx, "No consumer groups to restore")
		return nil
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := r.processTopics(ctx, grouped); err != nil {
			return err
		}
		if len(grouped) == 0 {
			slog.InfoContext(ctx, "All consumer groups restored")
			return nil
		}

		slog.InfoContext(ctx, "Waiting for next restore iteration", "remaining_topics", len(grouped))
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// processTopics iterates through all topics and processes their group offsets.
func (r *Restorer) processTopics(ctx context.Context, grouped map[string][]groupPartitionOffset) error {
	for topic, entries := range grouped {
		remaining, err := r.processTopic(ctx, topic, entries)
		if err != nil {
			return fmt.Errorf("processing topic %s: %w", topic, err)
		}
		if len(remaining) == 0 {
			delete(grouped, topic)
		} else {
			grouped[topic] = remaining
		}
	}
	return nil
}

// processTopic reads the latest record on each partition and resolves offsets for the group entries.
func (r *Restorer) processTopic(ctx context.Context, topic string, entries []groupPartitionOffset) ([]groupPartitionOffset, error) {
	restoredTopic := r.restoreTopicsPrefix + topic

	partitions := uniquePartitions(entries)
	latestRecords, err := kafkaint.ReadLatest(ctx, r.seedBrokers, r.tlsConfig, restoredTopic, partitions...)
	if err != nil {
		return entries, fmt.Errorf("reading latest records for %s: %w", restoredTopic, err)
	}

	var remaining []groupPartitionOffset
	for _, entry := range entries {
		rec, ok := latestRecords[entry.Partition]
		if !ok {
			remaining = append(remaining, entry)
			continue
		}

		resolved, err := r.resolveEntry(ctx, restoredTopic, rec, entry)
		if err != nil {
			return nil, err
		}
		if !resolved {
			remaining = append(remaining, entry)
		}
	}
	return remaining, nil
}

// resolveEntry checks if the latest record's source offset covers the entry's offset.
// If so, it finds the correct restored offset and commits it.
func (r *Restorer) resolveEntry(ctx context.Context, restoredTopic string, latestRecord *kgo.Record, entry groupPartitionOffset) (bool, error) {
	sourceOffset, err := topicsrestore.GetSourceOffsetFromHeader(latestRecord)
	if err != nil {
		return false, fmt.Errorf("getting source offset from header: %w", err)
	}

	if sourceOffset < entry.Offset {
		return false, nil
	}

	// Compute the new offset: entry.Offset - (sourceOffset - record.Offset)
	newOffset := entry.Offset - (sourceOffset - latestRecord.Offset)
	foundOffset, err := r.findRestoredOffset(ctx, restoredTopic, entry.Partition, newOffset, entry.Offset)
	if err != nil {
		return false, fmt.Errorf("finding restored offset: %w", err)
	}

	restoredGroupID := r.restoreGroupsPrefix + entry.GroupID
	if err := r.commitOffset(ctx, restoredGroupID, restoredTopic, entry.Partition, foundOffset); err != nil {
		return false, err
	}

	slog.InfoContext(ctx, "Restored consumer group offset",
		"group", restoredGroupID,
		"topic", restoredTopic,
		"partition", entry.Partition,
		"source_offset", entry.Offset,
		"restored_offset", foundOffset,
	)
	return true, nil
}

// findRestoredOffset reads the record at startOffset and walks forward until the
// restore.source-offset header matches the expected source offset.
func (r *Restorer) findRestoredOffset(ctx context.Context, topic string, partition int32, startOffset int64, expectedSourceOffset int64) (int64, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(r.seedBrokers...),
		kgo.DialTLSConfig(r.tlsConfig),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(startOffset)},
		}),
	)
	if err != nil {
		return -1, fmt.Errorf("creating kafka client: %w", err)
	}
	defer client.Close()

	for {
		fetches := client.PollRecords(ctx, 100)
		stop, err := kafkaint.HandleFetches(ctx, &fetches)
		if err != nil {
			return -1, fmt.Errorf("polling records: %w", err)
		}
		if stop {
			return -1, fmt.Errorf("context cancelled while searching for offset")
		}

		var found int64 = -1
		fetches.EachRecord(func(rec *kgo.Record) {
			if found >= 0 {
				return
			}
			srcOff, err := topicsrestore.GetSourceOffsetFromHeader(rec)
			if err != nil {
				return
			}
			if srcOff == expectedSourceOffset {
				found = rec.Offset
			}
		})

		if found >= 0 {
			return found, nil
		}
	}
}

// commitOffset commits a single offset for a consumer group.
func (r *Restorer) commitOffset(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{
		Topic:       topic,
		Partition:   partition,
		At:          offset,
		LeaderEpoch: -1,
	})

	_, err := r.kadmClient.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		return fmt.Errorf("committing offset for group %s topic %s partition %d: %w", groupID, topic, partition, err)
	}
	return nil
}

func uniquePartitions(entries []groupPartitionOffset) []int32 {
	seen := make(map[int32]bool)
	var result []int32
	for _, e := range entries {
		if !seen[e.Partition] {
			seen[e.Partition] = true
			result = append(result, e.Partition)
		}
	}
	return result
}
