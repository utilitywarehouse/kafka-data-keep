package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
)

type GroupWriter struct {
	client     *kadm.Client
	uploader   *ints3.Uploader
	encFactory codec.GroupEncoderFactory
	s3Location string
}

func NewGroupWriter(client *kadm.Client, uploader *ints3.Uploader, encFactory codec.GroupEncoderFactory, s3Location string) *GroupWriter {
	return &GroupWriter{
		client:     client,
		uploader:   uploader,
		encFactory: encFactory,
		s3Location: s3Location,
	}
}

func (w *GroupWriter) Backup(ctx context.Context) error {
	slog.InfoContext(ctx, "Starting consumer groups backup")

	groupIDs, err := w.client.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list groups: %w", err)
	}

	// Create temp file
	f, err := os.CreateTemp("", "consumer-groups-backup-*.avro")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := filepath.Clean(f.Name())
	defer func() {
		_ = f.Close()

		if err := os.Remove(tmpPath); err != nil {
			slog.WarnContext(ctx, "failed removing file", "error", err)
		}
	}()

	err = w.writeGroupsToFile(ctx, f, groupIDs)
	if err != nil {
		return err
	}

	if err := w.uploader.Upload(ctx, tmpPath, w.s3Location); err != nil {
		return fmt.Errorf("failed to upload backup: %w", err)
	}

	slog.InfoContext(ctx, "Uploaded consumer groups backup", "key", w.s3Location)
	return nil
}

func (w *GroupWriter) writeGroupsToFile(ctx context.Context, f *os.File, groupIDs kadm.ListedGroups) error {
	emptyPartitions, err := w.emptyPartitions(ctx)
	if err != nil {
		return err
	}

	encoder, err := w.encFactory.New(f)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	for _, groupID := range groupIDs.Groups() {
		kOffset, err := w.client.FetchOffsets(ctx, groupID)
		if err != nil {
			return fmt.Errorf("failed to fetch offsets for group %s: %w", groupID, err)
		}

		cgOffset := toAvro(ctx, groupID, kOffset, emptyPartitions)

		if err := encoder.Encode(cgOffset); err != nil {
			return fmt.Errorf("failed to encode group offset: %w", err)
		}
	}

	if err := encoder.Flush(); err != nil {
		return fmt.Errorf("failed to flush encoder: %w", err)
	}
	// We need to close the encoder to ensure footer is written for OCF
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("failed to close encoder: %w", err)
	}
	// Close file to ensure flush to disk
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	return nil
}

func toAvro(ctx context.Context, groupID string, kOffset kadm.OffsetResponses, emptyPartitions map[string]bool) *codec.ConsumerGroupOffset {
	cgOffset := &codec.ConsumerGroupOffset{
		GroupID: groupID,
		Topics:  make([]codec.TopicOffset, 0, len(kOffset)),
	}

	for topic, partitions := range kOffset {

		to := codec.TopicOffset{
			Topic:      topic,
			Partitions: make([]codec.PartitionOffset, 0, len(partitions)),
		}

		for partition, offset := range partitions {
			if offset.At < 0 {
				continue // No valid offset
			}
			if emptyPartitions[topicPartitionKey(topic, partition)] {
				slog.InfoContext(ctx, "Skipping empty partition",
					"group", groupID, "topic", topic, "partition", partition)
				continue
			}

			po := codec.PartitionOffset{
				Partition: partition,
				Offset:    offset.At,
				LeaderEpoch: func() *int32 {
					if offset.LeaderEpoch != -1 {
						return &offset.LeaderEpoch
					}
					return nil
				}(),
				Metadata: func() *string {
					if offset.Metadata != "" {
						return &offset.Metadata
					}
					return nil
				}(),
			}
			to.Partitions = append(to.Partitions, po)
		}
		if len(to.Partitions) > 0 {
			cgOffset.Topics = append(cgOffset.Topics, to)
		}
	}
	return cgOffset
}

// emptyPartitions returns a set of topic-partitions where start offset equals
// end offset, meaning the partition contains no records. Keys are formatted as
// "topic/partition" via topicPartitionKey.
func (w *GroupWriter) emptyPartitions(ctx context.Context) (map[string]bool, error) {
	startOffsets, err := w.client.ListStartOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list start offsets: %w", err)
	}
	endOffsets, err := w.client.ListEndOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list end offsets: %w", err)
	}

	empty := make(map[string]bool)
	endOffsets.Each(func(eo kadm.ListedOffset) {
		so, ok := startOffsets.Lookup(eo.Topic, eo.Partition)
		if !ok {
			return
		}
		if so.Offset == eo.Offset {
			empty[topicPartitionKey(eo.Topic, eo.Partition)] = true
		}
	})
	return empty, nil
}

func topicPartitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}
