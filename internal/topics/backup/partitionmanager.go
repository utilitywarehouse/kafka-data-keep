package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/utilitywarehouse/kafka-data-keep/internal/s3"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/codec"
)

type writerConfig struct {
	MinFileSize            int64
	PartitionIdleThreshold time.Duration
	RootPath               string
	S3Prefix               string
}

type partitionsWriterManager struct {
	uploader       *s3.Uploader
	config         writerConfig
	encoderFactory codec.RecordEncoderFactory
	decoderFactory codec.RecordDecoderFactory

	mu      sync.Mutex
	writers map[string]*partitionWriter
}

func newPartitionsWriterManager(uploader *s3.Uploader, encoderFactory codec.RecordEncoderFactory, decoderFactory codec.RecordDecoderFactory, config writerConfig) (*partitionsWriterManager, error) {
	m := &partitionsWriterManager{
		uploader:       uploader,
		config:         config,
		encoderFactory: encoderFactory,
		decoderFactory: decoderFactory,
		writers:        make(map[string]*partitionWriter),
	}

	return m, nil
}

func (m *partitionsWriterManager) OnPartitionsAssigned(ctx context.Context, committer OffsetCommitter, partitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, parts := range partitions {
		for _, partition := range parts {
			key := partitionWriterKey(topic, partition)
			if _, exists := m.writers[key]; !exists {
				m.writers[key] = newPartitionWriter(m.uploader, committer, m.config, m.encoderFactory, m.decoderFactory, topic, partition)
			}
		}
	}

	// remove the folders for partitions that we do not currently own
	if err := m.cleanupFormerPartitionFolders(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to cleanup stale folders", "error", err)
	}
}

func (m *partitionsWriterManager) cleanupFormerPartitionFolders(ctx context.Context) error {
	baseDir := filepath.Join(m.config.RootPath, m.config.S3Prefix)

	// If the directory doesn't exist, there's nothing to clean up
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		return nil
	}

	topicEntries, err := os.ReadDir(baseDir)
	if err != nil {
		return fmt.Errorf("failed to read base directory %s: %w", baseDir, err)
	}

	for _, topicEntry := range topicEntries {
		if !topicEntry.IsDir() {
			continue
		}
		topic := topicEntry.Name()
		topicDir := filepath.Join(baseDir, topic)

		partEntries, err := os.ReadDir(topicDir)
		if err != nil {
			slog.WarnContext(ctx, "failed to read topic directory", "dir", topicDir, "error", err)
			continue
		}

		for _, partEntry := range partEntries {
			if !partEntry.IsDir() {
				continue
			}

			// Partition directory names should be integers
			partitionID, err := strconv.ParseInt(partEntry.Name(), 10, 32)
			if err != nil {
				// ignore non-integer directories as they might not be partition folders
				slog.DebugContext(ctx, "skipping non-integer directory in topic folder", "dir", partEntry.Name(), "topic", topic)
				continue
			}

			key := partitionWriterKey(topic, int32(partitionID))

			// Check if we have a writer for this partition
			if _, exists := m.writers[key]; !exists {
				dirToRemove := filepath.Join(topicDir, partEntry.Name())
				slog.InfoContext(ctx, "removing stale partition folder", "path", dirToRemove)
				if err := os.RemoveAll(dirToRemove); err != nil {
					slog.ErrorContext(ctx, "failed to remove stale partition folder", "path", dirToRemove, "error", err)
				}
			}
		}
	}
	return nil
}

func (m *partitionsWriterManager) OnPartitionsRevoked(ctx context.Context, partitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, parts := range partitions {
		for _, partition := range parts {
			key := partitionWriterKey(topic, partition)
			w, exists := m.writers[key]
			if exists {
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

func (m *partitionsWriterManager) OnPartitionLost(ctx context.Context, partitions map[string][]int32) {
	m.OnPartitionsRevoked(ctx, partitions)
}

func (m *partitionsWriterManager) GetWriter(topic string, partition int32) (*partitionWriter, error) {
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

func (m *partitionsWriterManager) Close() error {
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

func (m *partitionsWriterManager) PauseIdleWriters(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, w := range m.writers {
		if err := w.PauseWhenIdle(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
