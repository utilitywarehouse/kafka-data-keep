package backup

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

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
	partition       int

	mu                    sync.Mutex
	currentEncoder        codec.RecordEncoder
	currentCountingWriter *countingWriter
	currentFilePath       string // Full local path
	currentKey            string // S3 key (relative path)

	firstOffset int64
	lastOffset  int64

	// We need to track if we have an open file
	isOpen bool
}

func NewPartitionWriter(uploader *Uploader, offsetCommitter OffsetCommitter, config Config, encoderFactory codec.RecordEncoderFactory, topic string, partition int) *PartitionWriter {
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

	for _, record := range records {
		if err := p.currentEncoder.Encode(record); err != nil {
			return fmt.Errorf("failed to encode record for partition %s-%d: %w", p.topic, p.partition, err)
		}

		p.lastOffset = record.Offset
	}

	if p.shouldFlush() {
		if err := p.flushLocked(ctx); err != nil {
			return fmt.Errorf("failed to flush file for partition %s-%d: %w", p.topic, p.partition, err)
		}
	}

	return nil
}

func (p *PartitionWriter) open(offset int64) error {
	// Simple masking with 0 padding so that we get the file names in alphabetical order
	maskedOffset := fmt.Sprintf("%040d", offset)
	filename := fmt.Sprintf("%s-%d-%s.avro", p.topic, p.partition, maskedOffset)

	// Key for S3 (relative path)
	key := filepath.Join(p.topic, fmt.Sprintf("%d", p.partition), filename)
	p.currentKey = key

	// Full local path
	localPath := filepath.Join(p.config.RootPath, key)
	p.currentFilePath = localPath

	// Ensure directory exists
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// overwrite file if it exists already
	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}

	// Wrap writer to count bytes
	cw := &countingWriter{w: f}

	enc, err := p.encoderFactory.New(cw)
	if err != nil {
		ferr := f.Close()
		if ferr != nil {
			slog.Error("failed closing the opened file", "error", ferr)
		}
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	p.currentEncoder = enc
	p.currentCountingWriter = cw

	p.firstOffset = offset
	p.isOpen = true

	return nil
}

func (p *PartitionWriter) shouldFlush() bool {
	if !p.isOpen {
		return false
	}
	return p.currentCountingWriter.count >= p.config.FileSize
}

func (p *PartitionWriter) flushLocked(ctx context.Context) error {
	if !p.isOpen {
		return nil
	}

	if err := p.currentEncoder.Close(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	if err := p.currentCountingWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Upload the closed file
	if err := p.uploader.Upload(ctx, p.currentFilePath, p.currentKey); err != nil {
		return fmt.Errorf("failed to upload file %s: %w", p.currentFilePath, err)
	}

	// Commit offset
	if err := p.commitOffset(ctx); err != nil {
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	p.isOpen = false
	p.currentEncoder = nil
	p.currentCountingWriter = nil

	// Remove the local file after successful upload
	if err := os.Remove(p.currentFilePath); err != nil {
		return fmt.Errorf("failed to remove local file %s: %w", p.currentFilePath, err)
	}

	return nil
}

func (p *PartitionWriter) commitOffset(ctx context.Context) error {
	offsets := map[string]map[int32]kgo.EpochOffset{
		p.topic: {
			int32(p.partition): {
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

func (p *PartitionWriter) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.flushLocked(context.Background()); err != nil {
		return fmt.Errorf("failed to close writer for partition %s-%d: %w", p.topic, p.partition, err)
	}
	return nil
}

// Counts the bytes written to the underlying writer.
type countingWriter struct {
	w     io.WriteCloser
	count int64
}

func (c *countingWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.count += int64(n)
	return
}

func (c *countingWriter) Close() error {
	return c.w.Close()
}
