package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
)

type GroupWriter struct {
	client     *kadm.Client
	uploader   *ints3.Uploader
	encFactory codec.GroupEncoderFactory
	cfg        AppConfig
}

func NewGroupWriter(client *kadm.Client, uploader *ints3.Uploader, encFactory codec.GroupEncoderFactory, cfg AppConfig) *GroupWriter {
	return &GroupWriter{
		client:     client,
		uploader:   uploader,
		encFactory: encFactory,
		cfg:        cfg,
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
	defer func() {
		_ = f.Close()

		if err := os.Remove(f.Name()); err != nil {
			slog.WarnContext(ctx, "failed removing file", "error", err)
		}
	}()

	err = w.writeGroupsToFile(ctx, f, groupIDs)
	if err != nil {
		return err
	}

	if err := w.uploader.Upload(ctx, f.Name(), w.cfg.S3Location); err != nil {
		return fmt.Errorf("failed to upload backup: %w", err)
	}

	slog.InfoContext(ctx, "Uploaded consumer groups backup", "key", w.cfg.S3Location)
	return nil
}

func (w *GroupWriter) writeGroupsToFile(ctx context.Context, f *os.File, groupIDs kadm.ListedGroups) error {
	encoder, err := w.encFactory.New(f)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	for _, groupID := range groupIDs.Groups() {
		kOffset, err := w.client.FetchOffsets(ctx, groupID)
		if err != nil {
			return fmt.Errorf("failed to fetch offsets for group %s: %w", groupID, err)
		}

		cgOffset := toAvro(groupID, kOffset)

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

func toAvro(groupID string, kOffset kadm.OffsetResponses) *codec.ConsumerGroupOffset {
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
				continue // No valid offset?
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
		cgOffset.Topics = append(cgOffset.Topics, to)
	}
	return cgOffset
}
