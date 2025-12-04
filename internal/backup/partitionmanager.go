package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/utilitywarehouse/kafka-data-keep/internal/codec"
)

type Config struct {
	MinFileSize int64
	RootPath    string
	S3Prefix    string
}

type PartitionsWriterManager struct {
	uploader       *Uploader
	config         Config
	encoderFactory codec.RecordEncoderFactory

	mu      sync.Mutex
	writers map[string]*PartitionWriter
}

func NewPartitionsWriterManager(uploader *Uploader, encoderFactory codec.RecordEncoderFactory, config Config) (*PartitionsWriterManager, error) {
	m := &PartitionsWriterManager{
		uploader:       uploader,
		config:         config,
		encoderFactory: encoderFactory,
		writers:        make(map[string]*PartitionWriter),
	}

	return m, nil
}

func (m *PartitionsWriterManager) OnPartitionsAssigned(committer OffsetCommitter, partitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, parts := range partitions {
		for _, partition := range parts {
			key := partitionWriterKey(topic, partition)
			if _, exists := m.writers[key]; !exists {
				m.writers[key] = NewPartitionWriter(m.uploader, committer, m.config, m.encoderFactory, topic, partition)
			}
		}
	}
}

func (m *PartitionsWriterManager) OnPartitionsRevoked(ctx context.Context, partitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, parts := range partitions {
		for _, partition := range parts {
			key := partitionWriterKey(topic, partition)
			w, exists := m.writers[key]
			if exists {
				//nolint: contextcheck
				if err := w.Close(); err != nil {
					slog.ErrorContext(ctx, "failed to close partition writer on revocation", "error", err)
				}
				delete(m.writers, key)
			} else {
				slog.WarnContext(ctx, "partition writer not found on revocation", "topic", topic, "partition", partition)
			}
		}
	}
}

func (m *PartitionsWriterManager) OnPartitionLost(ctx context.Context, partitions map[string][]int32) {
	m.OnPartitionsRevoked(ctx, partitions)
}

func (m *PartitionsWriterManager) GetWriter(topic string, partition int32) (*PartitionWriter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := partitionWriterKey(topic, partition)
	w, ok := m.writers[key]
	if !ok {
		return nil, fmt.Errorf("writer not found for partition %s-%d", topic, partition)
	}
	return w, nil
}

func partitionWriterKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func (m *PartitionsWriterManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, w := range m.writers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
