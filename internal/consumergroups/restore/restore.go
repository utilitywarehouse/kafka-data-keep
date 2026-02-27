// Package restore implements consumer group offset restoration from S3 backups.
package restore

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	topicsrestore "github.com/utilitywarehouse/kafka-data-keep/internal/topics/restore"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

// groupOffset holds a single consumer group's offset for one partition.
type groupOffset struct {
	GroupID   string
	Topic     string
	Partition int32
	Offset    int64
}

// Restorer performs consumer group offset restoration.
type Restorer struct {
	kadmClient          *kadm.Client
	restoreGroupsPrefix string
	restoreTopicsPrefix string
	consumeClient       *kgo.Client
	consumeMu           sync.Mutex
	latestReader        *kafkaint.LatestReader
}

// NewRestorer creates a new Restorer.
func NewRestorer(client *kafka.Client, restoreGroupsPrefix, restoreTopicsPrefix string) (*Restorer, error) {
	// read the connection config from the initialised client
	seedBrokers := client.OptValue(kgo.SeedBrokers).([]string)    //nolint:errcheck // this would fail only if the franz-go lib changes, and we'll catch that in integration tests
	tlsConfig := client.OptValue(kgo.DialTLSConfig).(*tls.Config) //nolint:errcheck // this would fail only if the franz-go lib changes, and we'll catch that in integration tests

	consumeClient, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.DialTLSConfig(tlsConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("creating consume client: %w", err)
	}
	latestReader, err := kafkaint.NewLatestReader(seedBrokers, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("creating latest reader: %w", err)
	}

	return &Restorer{
		kadmClient:          kadm.NewClient(client.Client),
		restoreGroupsPrefix: restoreGroupsPrefix,
		restoreTopicsPrefix: restoreTopicsPrefix,
		consumeClient:       consumeClient,
		latestReader:        latestReader,
	}, nil
}

// Close shuts down the Restorer's shared consume client and latest reader.
func (r *Restorer) Close() {
	r.consumeClient.Close()
	r.latestReader.Close()
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

		filtered := filterTopicPartitions(ctx, cg, fetched)
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
				})
			}
		}
	}
	return grouped
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

	if sourceOffset < entry.Offset {
		// we're not there yet
		slog.InfoContext(ctx, "Latest record is not yet beyond the group offset", "entry", entry, "latest_record_source_offset", sourceOffset)
		return false, nil
	}

	// Compute the new offset: entry.Offset - (sourceOffset - record.Offset)
	newOffset := entry.Offset - (sourceOffset - latestRecord.Offset)
	foundOffset, err := r.findRestoredOffset(ctx, entry, newOffset)
	if err != nil {
		return false, fmt.Errorf("finding restored offset: %w", err)
	}

	if foundOffset == -1 {
		return false, nil
	}

	restoredGroupID := r.restoreGroupsPrefix + entry.GroupID
	if err := r.commitOffset(ctx, restoredGroupID, r.restoredTopic(entry.Topic), entry.Partition, foundOffset); err != nil {
		return false, err
	}

	slog.InfoContext(ctx, "Restored consumer group offset", "group_entry", entry, "restored_offset", foundOffset)
	return true, nil
}

// findRestoredOffset acquires the consume client mutex and delegates to searchOffset.
func (r *Restorer) findRestoredOffset(ctx context.Context, entry groupOffset, startOffset int64) (int64, error) {
	r.consumeMu.Lock()
	defer r.consumeMu.Unlock()

	return r.searchOffset(ctx, entry, startOffset, 5)
}

// searchAheadMax is used for the maximum number of records to fetch beyond the expected offset.
// Those records will be searched if the source offset is not on the expected record, but on the following ones.
// Using a moderate number to not load the memory too much, but to still make use of the Kafka fetch if needed.
const searchAheadMax = 500

// searchOffset reads records starting at startOffset and walks forward until the
// restore.source-offset header matches groupOffset.
// It must be called with consumeMu already held.
func (r *Restorer) searchOffset(ctx context.Context, entry groupOffset, startOffset int64, depth int) (int64, error) {
	if depth <= 0 {
		return -1, fmt.Errorf("recursion depth limit reached while searching for offset for entry %v", entry)
	}
	topic := r.restoredTopic(entry.Topic)
	r.consumeClient.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {entry.Partition: kgo.NewOffset().At(startOffset)},
	})

	fetches := r.consumeClient.PollRecords(ctx, searchAheadMax)
	if err := fetches.Err0(); err != nil {
		return -1, fmt.Errorf("fetching from kafka: %w", err)
	}

	// reset the consume client so we can reuse it
	r.consumeClient.RemoveConsumePartitions(map[string][]int32{topic: {entry.Partition}})
	r.consumeClient.PurgeTopicsFromClient(topic)

	recs := fetches.Records()
	if len(recs) == 0 {
		return -1, fmt.Errorf("didn't read any records on poll")
	}

	// we expect to have the expected consumer group offset on the record at the start offset
	firstRec := recs[0]
	firstRecSrcOffset, err := topicsrestore.GetSourceOffsetFromHeader(firstRec)
	if err != nil {
		return -1, fmt.Errorf("reading first record source offset: %w", err)
	}

	if firstRecSrcOffset == entry.Offset {
		slog.DebugContext(ctx, "Found group offset on the restored record as expected",
			"group_entry", entry, "restored_record_offset", firstRec.Offset)
		return firstRec.Offset, nil
	}

	if entry.Offset < firstRecSrcOffset {
		searchNextOffset := startOffset - (firstRecSrcOffset - entry.Offset)
		slog.WarnContext(ctx, "Unexpected situation: the searched group offset is before the expected restored offset. Searching previous records",
			"group_entry", entry,
			"restored_record_offset", firstRec.Offset,
			"restored_record_source_offset", firstRecSrcOffset,
			"search_next_offset", searchNextOffset)

		return r.searchOffset(ctx, entry, searchNextOffset, depth-1)
	}

	slog.WarnContext(ctx, "Unexpected situation: the searched group offset is after the expected restored offset. Searching next fetched records",
		"group_entry", entry, "restored_record_offset", firstRec.Offset,
		"restored_record_source_offset", firstRecSrcOffset)

	for i, rec := range recs {
		srcOff, err := topicsrestore.GetSourceOffsetFromHeader(rec)
		if err != nil {
			return -1, fmt.Errorf("reading source offset: %w", err)
		}
		if entry.Offset == srcOff {
			slog.InfoContext(ctx, "Found group offset on the next fetched records",
				"group_entry", entry, "restored_record_offset", rec.Offset)
			return rec.Offset, nil
		}
		if entry.Offset < srcOff {
			// we reached an impossible situation where the consumer group offset is not among the restored records. Doing our best and setting the offset to the previously restored offset.
			previousRecord := recs[i-1]
			prevSrcOff, err := topicsrestore.GetSourceOffsetFromHeader(previousRecord)
			if err != nil {
				return -1, fmt.Errorf("reading previous source offset: %w", err)
			}

			slog.WarnContext(ctx, "Unexpected situation: the searched group offset doesn't exist in the restored records. Setting the group to the previously restored offset",
				"group_entry", entry, "previous_record_offset", previousRecord.Offset,
				"previous_record_source_offset", prevSrcOff)
			return previousRecord.Offset, nil
		}
	}

	// do another fetch and look for the offset in the next batch of fetched records until we find it
	return r.searchOffset(ctx, entry, recs[len(recs)-1].Offset, depth-1)
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
