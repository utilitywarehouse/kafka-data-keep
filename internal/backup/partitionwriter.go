package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec"
)

// OffsetCommitter defines the interface for committing offsets.
type OffsetCommitter interface {
	CommitOffsets(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset, onDone func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error))
}

type PartitionWriter struct {
	uploader        *Uploader
	offsetCommitter OffsetCommitter
	config          Config
	encoderFactory  codec.RecordEncoderFactory
	topic           string
	partition       int32

	mu              sync.Mutex
	currentEncoder  codec.RecordEncoder
	currentFile     *os.File
	currentFilePath string // Full local path
	currentKey      string // S3 key (relative path)

	firstOffset int64
	lastOffset  int64
	lastWriteAt time.Time

	// We need to track if we have an open file
	isOpen bool
}

func NewPartitionWriter(uploader *Uploader, offsetCommitter OffsetCommitter, config Config, encoderFactory codec.RecordEncoderFactory, topic string, partition int32) *PartitionWriter {
	return &PartitionWriter{
		uploader:        uploader,
		offsetCommitter: offsetCommitter,
		config:          config,
		encoderFactory:  encoderFactory,
		topic:           topic,
		partition:       partition,
	}
}

func (p *PartitionWriter) WriteRecords(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isOpen {
		if err := p.open(records[0].Offset); err != nil {
			return fmt.Errorf("failed to open file for partition %s-%d: %w", p.topic, p.partition, err)
		}
	}

	// check if we need to resume writing to the file
	if p.isPaused() {
		if err := p.resumeLocalFile(ctx); err != nil {
			return fmt.Errorf("failed to resume local file for partition %s-%d: %w", p.topic, p.partition, err)
		}
	}

	for _, record := range records {
		if err := p.currentEncoder.Encode(record); err != nil {
			return fmt.Errorf("failed to encode record for partition %s-%d: %w", p.topic, p.partition, err)
		}

		p.lastOffset = record.Offset
	}

	p.lastWriteAt = time.Now()
	shouldFlush, err := p.shouldFlush()
	if err != nil {
		return fmt.Errorf("failed to check if file should be flushed for partition %s-%d: %w", p.topic, p.partition, err)
	}
	if shouldFlush {
		if err := p.flushLocked(ctx); err != nil {
			return fmt.Errorf("failed to flush file for partition %s-%d: %w", p.topic, p.partition, err)
		}
	}

	return nil
}

func (p *PartitionWriter) open(offset int64) error {
	// Simple masking with 0 padding so that we get the file names in alphabetical order. 19 digits is the maximum we can have in kafka offsets.
	// Using the first offset in the file name to ensure idempotence when something fails during execution until we commit the offsets after file upload.
	maskedOffset := fmt.Sprintf("%019d", offset)
	filename := fmt.Sprintf("%s-%d-%s.avro", p.topic, p.partition, maskedOffset)

	// Key for S3 (relative path)
	key := filepath.Join(p.config.S3Prefix, p.topic, fmt.Sprintf("%d", p.partition), filename)
	p.currentKey = key

	// Full local path
	localPath := filepath.Join(p.config.RootPath, key)
	p.currentFilePath = localPath

	// Ensure directory exists
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// overwrite file if it exists already
	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}

	enc, err := p.encoderFactory.New(f)
	if err != nil {
		ferr := f.Close()
		if ferr != nil {
			slog.Error("failed closing the opened file", "error", ferr)
		}
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	p.currentFile = f
	p.currentEncoder = enc

	p.firstOffset = offset
	p.isOpen = true
	p.lastWriteAt = time.Time{}
	return nil
}

func (p *PartitionWriter) resumeLocalFile(ctx context.Context) error {
	slog.DebugContext(ctx, "resuming local file", "filename", p.currentFilePath)

	f, err := os.OpenFile(p.currentFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open file for resuming %s: %w", p.currentFilePath, err)
	}

	enc, err := p.encoderFactory.New(f)
	if err != nil {
		ferr := f.Close()
		if ferr != nil {
			slog.ErrorContext(ctx, "failed closing the opened file at resume", "error", ferr)
		}
		return fmt.Errorf("failed to create encoder on resume: %w", err)
	}
	p.currentEncoder = enc
	p.currentFile = f

	return nil
}

func (p *PartitionWriter) shouldFlush() (bool, error) {
	if !p.isOpen {
		return false, nil
	}
	if err := p.currentEncoder.Flush(); err != nil {
		return false, fmt.Errorf("failed to flush encoder: %w", err)
	}
	fstat, err := p.currentFile.Stat()
	if err != nil {
		return false, fmt.Errorf("failed to stat file: %w", err)
	}
	return fstat.Size() >= p.config.MinFileSize, nil
}

func (p *PartitionWriter) flushLocked(ctx context.Context) error {
	if !p.isOpen {
		return nil
	}

	slog.DebugContext(ctx, "Flushing file for partition", "filename", p.currentKey)

	// check if the writer was paused
	if !p.isPaused() {
		if err := p.closeLocalFileLocked(); err != nil {
			return fmt.Errorf("failed closing local file on flush: %w", err)
		}
	}
	// Upload the closed file
	if err := p.uploader.Upload(ctx, p.currentFilePath, p.currentKey); err != nil {
		return fmt.Errorf("failed to upload file %s: %w", p.currentFilePath, err)
	}

	// Remove the local file after successful upload
	if err := os.Remove(p.currentFilePath); err != nil {
		return fmt.Errorf("failed to remove local file %s: %w", p.currentFilePath, err)
	}

	// Commit offset
	if err := p.commitOffset(ctx); err != nil {
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	p.isOpen = false
	p.lastWriteAt = time.Time{}

	return nil
}

func (p *PartitionWriter) isPaused() bool {
	return p.currentEncoder == nil
}

func (p *PartitionWriter) commitOffset(ctx context.Context) error {
	offsets := map[string]map[int32]kgo.EpochOffset{
		p.topic: {
			p.partition: {
				Epoch:  -1, // Unknown epoch
				Offset: p.lastOffset + 1,
			},
		},
	}

	// do the commit synchronously
	var commitErr error
	done := make(chan struct{})
	p.offsetCommitter.CommitOffsets(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
		close(done)
	})
	<-done

	return commitErr
}

func (p *PartitionWriter) PauseWhenIdle(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.isOpen || !p.isIdle() {
		return nil
	}

	slog.DebugContext(ctx, "closing local file as idle", "filename", p.currentKey)

	// close the local file when the writer is idle
	if err := p.closeLocalFileLocked(); err != nil {
		return fmt.Errorf("failed closing local file on idle: %w", err)
	}

	return nil
}

func (p *PartitionWriter) closeLocalFileLocked() error {
	// close the encoder first, so that it will flush
	if err := p.currentEncoder.Close(); err != nil {
		return fmt.Errorf("failed closing local encoder: %w", err)
	}

	if err := p.currentFile.Close(); err != nil {
		return fmt.Errorf("failed closing file: %w", err)
	}

	p.currentEncoder = nil
	p.currentFile = nil

	return nil
}

func (p *PartitionWriter) isIdle() bool {
	return p.currentEncoder != nil && !p.lastWriteAt.IsZero() &&
		p.lastWriteAt.Add(p.config.PartitionIdleThreshold).Before(time.Now())
}

func (p *PartitionWriter) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// don't do a full flush, just close the local file
	if !p.isOpen || p.isPaused() {
		return nil
	}

	if err := p.closeLocalFileLocked(); err != nil {
		return fmt.Errorf("failed to close writer for partition %s-%d: %w", p.topic, p.partition, err)
	}

	return nil
}
