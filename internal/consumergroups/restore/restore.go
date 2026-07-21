// Package restore implements consumer group offset restoration from S3 backups.
package restore

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	topicsrestore "github.com/utilitywarehouse/kafka-data-keep/internal/topics/restore"
)

// groupOffset holds a single consumer group's offset for one partition.
type groupOffset struct {
	GroupID             string
	Topic               string
	Partition           int32
	Offset              int64
	LastProcessedOffset int64
}

// Restorer performs consumer group offset restoration.
type Restorer struct {
	kadmClient           *kadm.Client
	restoreGroupsPrefix  string
	restoreTopicsPrefix  string
	consumeClient        *kgo.Client
	consumeMu            sync.Mutex
	latestReader         *kafka.LatestReader
	loopInterval         time.Duration
	excludeTopicsRegexes []*regexp.Regexp
}

// Restore orchestrates the full consumer group offset restoration.
func (r *Restorer) Restore(ctx context.Context, offsets []codec.ConsumerGroupOffset) error {
	for _, cg := range offsets {
		for _, to := range cg.Topics {
			for _, po := range to.Partitions {
				recordStatusMetric(ctx, cg.GroupID, to.Topic, po.Partition, statusScheduled)
			}
		}
	}

	remaining, err := r.filterAlreadyRestored(ctx, offsets)
	if err != nil {
		return fmt.Errorf("checking already restored groups: %w", err)
	}
	slog.InfoContext(ctx, "Filtered already restored groups", "remaining", len(remaining))

	grouped := groupByTopic(remaining)
	slog.InfoContext(ctx, "Grouped by topics", "total", len(grouped))

	grouped = filterExcludedTopics(ctx, grouped, r.excludeTopicsRegexes)
	slog.InfoContext(ctx, "Filtered excluded topics", "remaining", len(grouped))

	grouped, err = r.filterNonExistingTopics(ctx, grouped)
	if err != nil {
		return fmt.Errorf("filtering non existing topics: %w", err)
	}
	slog.InfoContext(ctx, "Filtered non existing topics", "remaining", len(grouped))

	for _, entries := range grouped {
		for _, e := range entries {
			recordStatusMetric(ctx, e.GroupID, e.Topic, e.Partition, statusInProgress)
		}
	}

	return r.runLoop(ctx, r.loopInterval, grouped)
}

// filterAlreadyRestored removes partitions that already have offsets committed
// in the Kafka cluster (using the prefixed group name), keeping only the ones not yet restored.
func (r *Restorer) filterAlreadyRestored(ctx context.Context, offsets []codec.ConsumerGroupOffset) ([]codec.ConsumerGroupOffset, error) {
	// Collect all prefixed group IDs to fetch offsets in a single batch call.
	groups := make([]string, len(offsets))
	for i, cg := range offsets {
		groups[i] = r.restoredGroup(cg.GroupID)
	}

	fetchedAll := r.kadmClient.FetchManyOffsets(ctx, groups...)
	if err := fetchedAll.Error(); err != nil {
		return nil, fmt.Errorf("failed fetching offsets for groups %v: %w", groups, err)
	}

	var result []codec.ConsumerGroupOffset
	for _, cg := range offsets {
		restoredGroupID := r.restoredGroup(cg.GroupID)
		fetched := fetchedAll[restoredGroupID].Fetched

		if len(fetched) == 0 {
			slog.DebugContext(ctx, "Including all partitions from not existing group", "group", restoredGroupID)
			result = append(result, cg)
			continue
		}

		slog.DebugContext(ctx, "Got fetched partitions for group", "group", restoredGroupID, "partitions", fetched)

		filtered := r.filterTopicPartitions(ctx, cg, fetched)
		if len(filtered.Topics) > 0 {
			result = append(result, filtered)
		} else {
			slog.InfoContext(ctx, "Skipping fully restored group", "group", restoredGroupID)
		}
	}
	return result, nil
}

func (r *Restorer) restoredGroup(groupName string) string {
	return r.restoreGroupsPrefix + groupName
}

// filterTopicPartitions removes partitions that already have committed offsets from a consumer group.
func (r *Restorer) filterTopicPartitions(ctx context.Context, cg codec.ConsumerGroupOffset, fetched kadm.OffsetResponses) codec.ConsumerGroupOffset {
	filtered := codec.ConsumerGroupOffset{GroupID: cg.GroupID}
	for _, to := range cg.Topics {
		fetchedPartitions := fetched[r.restoredTopic(to.Topic)]
		var remaining []codec.PartitionOffset
		for _, po := range to.Partitions {
			if fetchedPartitions != nil {
				if resp, ok := fetchedPartitions[po.Partition]; ok && resp.At >= 0 {
					slog.DebugContext(ctx, "Skipping already restored partition",
						"group", cg.GroupID, "topic", to.Topic, "partition", po.Partition)
					recordStatusMetric(ctx, cg.GroupID, to.Topic, po.Partition, statusAlreadyRestored)
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
func groupByTopic(offsets []codec.ConsumerGroupOffset) map[string][]groupOffset {
	grouped := make(map[string][]groupOffset)
	for _, cg := range offsets {
		for _, to := range cg.Topics {
			for _, po := range to.Partitions {
				grouped[to.Topic] = append(grouped[to.Topic], groupOffset{
					GroupID:   cg.GroupID,
					Topic:     to.Topic,
					Partition: po.Partition,
					Offset:    po.Offset,
					// This is because the consumer group offset points to the next record it will read, so the last processed record offset is the previous one
					LastProcessedOffset: po.Offset - 1,
				})
			}
		}
	}
	return grouped
}

// filterExcludedTopics removes topics that match any of the provided exclude regexes.
func filterExcludedTopics(ctx context.Context, grouped map[string][]groupOffset, excludeRegexes []*regexp.Regexp) map[string][]groupOffset {
	if len(excludeRegexes) == 0 {
		return grouped
	}
	result := make(map[string][]groupOffset, len(grouped))
	for topic, entries := range grouped {
		if internal.MatchesAny(topic, excludeRegexes) {
			slog.InfoContext(ctx, "Skipping excluded topic", "topic", topic)
			for _, e := range entries {
				recordStatusMetric(ctx, e.GroupID, e.Topic, e.Partition, statusSkipped)
			}
			continue
		}
		result[topic] = entries
	}
	return result
}

// filterNonExistingTopics keeps only topics that exist in the Kafka cluster (with restore prefix).
func (r *Restorer) filterNonExistingTopics(ctx context.Context, grouped map[string][]groupOffset) (map[string][]groupOffset, error) {
	requestedTopics := make([]string, 0, len(grouped))
	for topic := range grouped {
		requestedTopics = append(requestedTopics, r.restoredTopic(topic))
	}

	metadata, err := r.kadmClient.ListTopics(ctx, requestedTopics...)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata for topics: %w", err)
	}

	existingTopics := make(map[string]bool, len(metadata))
	for _, t := range metadata {
		if !t.IsInternal && t.Err == nil {
			existingTopics[t.Topic] = true
		}
	}

	result := make(map[string][]groupOffset)
	for topic, entries := range grouped {
		restoredTopic := r.restoredTopic(topic)
		if existingTopics[restoredTopic] {
			result[topic] = entries
		} else {
			slog.InfoContext(ctx, "Skipping topic not found in cluster", "topic", restoredTopic)
			for _, e := range entries {
				recordStatusMetric(ctx, e.GroupID, e.Topic, e.Partition, statusSkipped)
			}
		}
	}
	return result, nil
}

// runLoop runs the restore process on a ticker until all offsets are resolved or context is cancelled.
func (r *Restorer) runLoop(ctx context.Context, interval time.Duration, grouped map[string][]groupOffset) error {
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
		for topic, entries := range grouped {
			slog.DebugContext(ctx, "Remaining group offsets", "topic", topic, "offsets", entries)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// processTopics iterates through all topics and processes their group offsets.
func (r *Restorer) processTopics(ctx context.Context, grouped map[string][]groupOffset) error {
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
func (r *Restorer) processTopic(ctx context.Context, topic string, entries []groupOffset) ([]groupOffset, error) {
	slog.InfoContext(ctx, "Start processing topic", "topic", topic, "entries", entries)
	restoredTopic := r.restoredTopic(topic)

	partitions := uniquePartitions(entries)

	latestRecords, err := r.latestReader.Read(ctx, restoredTopic, partitions...)
	if err != nil {
		return entries, fmt.Errorf("reading latest records for %s: %w", restoredTopic, err)
	}

	var remaining []groupOffset
	for _, entry := range entries {
		rec, ok := latestRecords[entry.Partition]
		if !ok {
			slog.InfoContext(ctx, "Skipping entry with no latest record", "entry", entry)
			remaining = append(remaining, entry)
			continue
		}

		resolved, err := r.resolveEntry(ctx, entry, rec)
		if err != nil {
			return nil, err
		}
		if !resolved {
			slog.InfoContext(ctx, "Could not resolve yet the offset for entry", "entry", entry)
			remaining = append(remaining, entry)
		}
	}
	return remaining, nil
}

func (r *Restorer) restoredTopic(topic string) string {
	return r.restoreTopicsPrefix + topic
}

// resolveEntry checks if the latest record's source offset covers the entry's offset.
// If so, it finds the correct restored offset and commits it.
func (r *Restorer) resolveEntry(ctx context.Context, entry groupOffset, latestRecord *kgo.Record) (bool, error) {
	sourceOffset, err := topicsrestore.GetSourceOffsetFromHeader(latestRecord)
	if err != nil {
		return false, fmt.Errorf("getting source offset from header: %w", err)
	}

	if sourceOffset == -1 {
		slog.InfoContext(ctx, "Could not find the source offset header on latest record", "entry", entry,
			"latest_record_offset", latestRecord.Offset, "latest_record_headers", latestRecord.Headers)
		return false, nil
	}

	// We're looking for the offset of the last processed entry.
	// This is because the consumer group offset points to the next record it will read, and when it is at the tip (i.e. it consumed everything in that partition), that record doesn't exist in the partition yet.
	if sourceOffset < entry.LastProcessedOffset {
		// we're not there yet
		slog.InfoContext(ctx, "Latest record is not yet beyond the group offset", "entry", entry, "latest_record_source_offset", sourceOffset)
		return false, nil
	}

	// Compute the new offset: lastProcessedOffset - (sourceOffset - record.Offset)
	newOffset := entry.LastProcessedOffset - (sourceOffset - latestRecord.Offset)
	foundOffset, err := r.translateLastProcessed(ctx, entry, newOffset, latestRecord.Offset)
	if err != nil {
		return false, fmt.Errorf("finding restored offset: %w", err)
	}

	// add 1 to the offset, as this is the previous record to where the consumer group is pointing to.
	foundOffset++
	restoredGroupID := r.restoreGroupsPrefix + entry.GroupID
	if err := r.commitOffset(ctx, restoredGroupID, r.restoredTopic(entry.Topic), entry.Partition, foundOffset); err != nil {
		return false, err
	}

	slog.InfoContext(ctx, "Restored consumer group offset", "group_entry", entry, "restored_offset", foundOffset)
	recordStatusMetric(ctx, entry.GroupID, entry.Topic, entry.Partition, statusRestored)
	return true, nil
}

// maxBackwardsIterations specifies how many times to look backwards relative to the expected offset
const maxBackwardsIterations = 4

// translateLastProcessed acquires the consume client mutex and delegates to searchLastProcessed.
func (r *Restorer) translateLastProcessed(ctx context.Context, entry groupOffset, startOffset int64, upperBoundOffset int64) (int64, error) {
	r.consumeMu.Lock()
	defer r.consumeMu.Unlock()

	return r.searchLastProcessed(ctx, entry, startOffset, upperBoundOffset, maxBackwardsIterations)
}

const (
	offsetStartOfPartition int64 = -1 // no last-processed; next-to-read will be 0 after +1
	offsetNotFound         int64 = -2 // value returned together with an error when the last processed offset could not be found
)

// searchLastProcessed reads records starting at startOffset and walks forward until the
// restore.source-offset header matches group last processed offset.
// It must be called with consumeMu already held.
func (r *Restorer) searchLastProcessed(ctx context.Context, entry groupOffset, startOffset int64, upperBoundOffset int64, backwardsIteration int) (int64, error) {
	topic := r.restoredTopic(entry.Topic)

	if backwardsIteration <= 0 {
		// do a search from 0 that will check if the group offset even exists in the partition, and will do a search forward afterwards
		return r.searchLastProcessed(ctx, entry, 0, upperBoundOffset, 1)
	}
	// A negative startOffset (e.g. computed delta overshoots partition start)
	// would be interpreted by franz-go as "consume from the end", causing
	// PollRecords to block indefinitely waiting for new records that never
	// arrive during a restore. Clamp to 0 so we scan from the beginning.
	if startOffset < 0 {
		slog.WarnContext(ctx, "Clamping negative startOffset to 0 in searchLastProcessed",
			"group_entry", entry, "startOffset", startOffset)
		startOffset = 0
	}

	// we expect to have the expected consumer group offset on the record at the start offset
	rec, srcOffset, err := r.fetchRecordAt(ctx, topic, entry.Partition, startOffset)
	if err != nil {
		return offsetNotFound, err
	}

	if srcOffset == entry.LastProcessedOffset {
		slog.DebugContext(ctx, "Found last processed source offset on the expected record",
			"group_entry", entry, "offset", rec.Offset)
		return rec.Offset, nil
	}

	if entry.LastProcessedOffset < srcOffset {
		searchNextOffset := startOffset - (srcOffset - entry.LastProcessedOffset)
		//  if we're at the start of the topic and the next offset to search is negative, just return the start of the partition
		if searchNextOffset < 0 && startOffset == 0 {
			slog.WarnContext(ctx, "Unexpected situation: the searched group offset is not in the restored data. It may have gotten deleted due to the retention policy on the backed up data. Setting the group at the start of partition",
				"group_entry", entry, "offset", rec.Offset,
				"source_offset", srcOffset, "deduced_invalid_group_offset", searchNextOffset)
			// returning -1 because there is no last processed, so the next record to be processed will be at offset 0
			return offsetStartOfPartition, nil
		}
		slog.WarnContext(ctx, "Unexpected situation: the searched group offset is before the expected restored offset. Searching previous records",
			"group_entry", entry, "offset", rec.Offset, "source_offset", srcOffset, "search_next_offset", searchNextOffset)
		return r.searchLastProcessed(ctx, entry, searchNextOffset, upperBoundOffset, backwardsIteration-1)
	}

	// partitions on compacted topics may contain highly irregular offsets restores, so it is safest to just look for it until it's found
	slog.WarnContext(ctx, "Unexpected situation: the searched group offset is after the expected restored offset. Scanning forward until found",
		"group_entry", entry, "offset", rec.Offset, "source_offset", srcOffset)

	return r.scanForwardLastProcessed(ctx, topic, entry, rec.Offset, upperBoundOffset)
}

// fetchRecordAt adds a consume partition, polls records, and always cleans up
// the consume client via defer so it can be reused for the next iteration.
func (r *Restorer) fetchRecordAt(ctx context.Context, topic string, partition int32, startOffset int64) (*kgo.Record, int64, error) {
	r.consumeClient.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {partition: kgo.NewOffset().At(startOffset)},
	})
	defer func() {
		r.consumeClient.RemoveConsumePartitions(map[string][]int32{topic: {partition}})
		r.consumeClient.PurgeTopicsFromClient(topic)
	}()

	fetches := r.consumeClient.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		return nil, 0, fmt.Errorf("fetching record at offset from kafka: %w", err)
	}

	recs := fetches.Records()

	if len(recs) == 0 {
		return nil, 0, fmt.Errorf("didn't read any records on poll")
	}

	srcOffset, err := topicsrestore.GetSourceOffsetFromHeader(recs[0])
	if err != nil {
		return nil, 0, fmt.Errorf("reading record at offset source offset: %w", err)
	}

	return recs[0], srcOffset, nil
}

// scanForwardMax is used for the maximum number of records to fetch when searching through all offsets.
// Those records will be searched if the source offset is not on the expected record, but on the following ones.
// Using a moderate number to not load the memory too much.
const scanForwardMax = 10000

// scanForwardLastProcessed continuously polls for records until the last processed offset is found or surpassed, or it reaches the maxOffset.
// It must be called with consumeMu already held.
func (r *Restorer) scanForwardLastProcessed(ctx context.Context, topic string, entry groupOffset, startOffset int64, maxOffset int64) (int64, error) {
	// Set up the consume partition once for any subsequent polls beyond the
	// initial batch. The consumer will continue from after the last record.
	r.consumeClient.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {entry.Partition: kgo.NewOffset().At(startOffset)},
	})
	defer func() {
		r.consumeClient.RemoveConsumePartitions(map[string][]int32{topic: {entry.Partition}})
		r.consumeClient.PurgeTopicsFromClient(topic)
	}()

	var recs []*kgo.Record
	for {
		// Poll the next batch directly — the consumer continues from where it left off.
		fetches := r.consumeClient.PollRecords(ctx, scanForwardMax)
		if err := fetches.Err0(); err != nil {
			return offsetNotFound, fmt.Errorf("polling from kafka: %w", err)
		}
		recs = fetches.Records()
		if len(recs) == 0 {
			return offsetNotFound, fmt.Errorf("no more records to scan while searching for offset for entry %v", entry)
		}

		for _, rec := range recs {
			if rec.Offset > maxOffset {
				return offsetNotFound, fmt.Errorf("could not find offset for entry: %+v and reached max offset %d", entry, maxOffset)
			}
			srcOff, err := topicsrestore.GetSourceOffsetFromHeader(rec)
			if err != nil {
				return offsetNotFound, fmt.Errorf("reading source offset: %w", err)
			}
			if entry.LastProcessedOffset == srcOff {
				slog.InfoContext(ctx, "Found last processed offset while scanning forward",
					"group_entry", entry, "offset", rec.Offset)
				return rec.Offset, nil
			}
			if entry.LastProcessedOffset < srcOff {
				// the consumer group offset is not among the restored records; we'll set the group to the next one
				slog.WarnContext(ctx, "Unexpected situation: the searched group offset doesn't exist in the restored records. Setting the group to the next record",
					"group_entry", entry, "next_record_offset", rec.Offset, "next_record_source_offset", srcOff)
				// subtracting 1, as this record wasn't processed yet
				return rec.Offset - 1, nil
			}
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

	resp, err := r.kadmClient.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		return fmt.Errorf("committing offset for group %s topic %s partition %d: %w", groupID, topic, partition, err)
	}
	if !resp.Ok() {
		return fmt.Errorf("committing offset for group %s topic %s partition %d: %w", groupID, topic, partition, resp.Error())
	}
	return nil
}

func uniquePartitions(entries []groupOffset) []int32 {
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
