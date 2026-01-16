package backup

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/codec/avro"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func init() {
	// Enable debug logging for slog
	slog.SetDefault(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
}

const (
	bucketName = "test-backup-bucket"
)

func TestBackupIntegration(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx := context.Background()

	kafkaBrokers, tkf := testutil.StartKafkaService(ctx, t)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := testutil.StartS3Service(ctx, t)
	t.Cleanup(ts3f)

	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(ctx, t, s3Endpoint)

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // set the partitions manually on produce
	)
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)

	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	t.Run("multiple batches per partitions", func(t *testing.T) {
		t.Parallel()

		topic1 := "multiple-1"
		topic2 := "multiple-2"
		_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic1)
		require.NoError(t, err)

		_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic2)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("test-backup-group")
		s3Prefix := "multiple-batches/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            "multiple-.*",
			GroupID:                groupID,
			MinFileSize:            5000,
			PartitionIdleThreshold: 100 * time.Millisecond,
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)

		// First batch of records
		writeRecords(t, ctx, adminClient, topic1, 0, 10, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic1, 1, 20, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 0, 20, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 1, 30, cfg.MinFileSize)
		require.NoError(t, adminClient.Flush(ctx))

		// Wait until these records are consumed
		waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 30, topic2: 50})

		// Second batch of records
		writeRecords(t, ctx, adminClient, topic1, 0, 20, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic1, 1, 10, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 0, 30, cfg.MinFileSize)
		writeRecords(t, ctx, adminClient, topic2, 1, 40, cfg.MinFileSize)
		require.NoError(t, adminClient.Flush(ctx))

		// Wait until the second batch is consumed
		waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic1: 60, topic2: 120})

		stopApp(ctx, t, cancel, errCh)

		expectedFiles := map[string]int{
			fileKey(s3Prefix, topic1, 0, 0):  10,
			fileKey(s3Prefix, topic1, 0, 10): 20,
			fileKey(s3Prefix, topic1, 1, 0):  20,
			fileKey(s3Prefix, topic1, 1, 20): 10,
			fileKey(s3Prefix, topic2, 0, 0):  20,
			fileKey(s3Prefix, topic2, 0, 20): 30,
			fileKey(s3Prefix, topic2, 1, 0):  30,
			fileKey(s3Prefix, topic2, 1, 30): 40,
		}

		filesFound := listFilesOnBucket(ctx, t, s3Client, s3Prefix)

		require.Equal(t, expectedFiles, filesFound)
	})

	t.Run("keep local files when stopping the app", func(t *testing.T) {
		t.Parallel()

		topic := "keep-local-on-stop-1"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("test-backup-group")
		s3Prefix := "keep-local-on-stop/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            topic,
			ExcludeTopicsRegex:     "multiple-.*", // exclude the topics from the previous test
			GroupID:                groupID,
			PartitionIdleThreshold: 1 * time.Second,
			MinFileSize:            100 * 1024 * 1024, // use a big limit, so we make sure no flush occurs
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// write records, and the file won't be flushed since the file size limit is very high
		writeRecords(t, ctx, adminClient, topic, 0, 1000, 1000)

		fileKey := fileKey(s3Prefix, topic, 0, 0)
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey, 1000)

		stopApp(ctx, t, cancel, errCh)

		// we expect no files on S3
		require.Empty(t, listFilesOnBucket(ctx, t, s3Client, s3Prefix), "no files should be on S3 after backup was stopped")
		// we expect that the local file is still there
		_, err := os.Stat(filepath.Join(workingDir, fileKey))
		require.NoError(t, err, "Local file should still exist after backup was stopped")
	})

	t.Run("overwrite local file when restarting the app", func(t *testing.T) {
		t.Parallel()

		topic := "overwrite-restart-1"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("test-overwrite-restart")
		s3Prefix := "overwrite-restart/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            topic,
			ExcludeTopicsRegex:     "multiple-.*", // exclude the topics from the previous test
			GroupID:                groupID,
			PartitionIdleThreshold: 1 * time.Second,
			MinFileSize:            100 * 1024 * 1024, // use a big limit, so we make sure no flush occurs
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// write records, but offsets won't be committed, since the file size limit is very high
		writeRecords(t, ctx, adminClient, topic, 0, 1000, 1000)
		require.NoError(t, adminClient.Flush(ctx))
		t.Logf("Wrote records to topic %s", topic)

		fileKey := fileKey(s3Prefix, topic, 0, 0)
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey, 1000)

		stopApp(ctx, t, cancel, errCh)

		backupCtx, cancel = context.WithCancel(ctx)
		defer cancel()

		// start the backup again
		errCh = make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// wait until the local file refills with all the records
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey, 1000)

		// write more records
		writeRecords(t, ctx, adminClient, topic, 0, 1000, 1000)

		// wait until the local file has the new records
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey, 2000)
		stopApp(ctx, t, cancel, errCh)
	})

	t.Run("remove leftover file on offset advance", func(t *testing.T) {
		t.Parallel()

		topic := "delete-old-local-on-offset-advance-1"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("delete-old-local-on-offset-advance")
		s3Prefix := "delete-old-local-on-offset-advance/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            topic,
			ExcludeTopicsRegex:     "multiple-.*", // exclude the topics from the previous test
			GroupID:                groupID,
			PartitionIdleThreshold: 1 * time.Second,
			MinFileSize:            100 * 1024 * 1024, // use a big limit, so we make sure no flush occurs
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// write records continuously, but offsets won't be committed, since the file size limit is very high
		writeRecords(t, ctx, adminClient, topic, 0, 1000, 1000)

		fileKey1 := fileKey(s3Prefix, topic, 0, 0)
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey1, 1000)

		stopApp(ctx, t, cancel, errCh)

		backupCtx, cancel = context.WithCancel(ctx)
		defer cancel()

		// delete 100 the offset from the start of the partition
		delMap := make(kadm.Offsets)
		delMap.Add(kadm.Offset{Topic: topic, Partition: 0, At: 100})
		_, err = kadmClient.DeleteRecords(ctx, delMap)
		require.NoError(t, err)

		// start the backup again
		errCh = make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)

		// a new local file should be started, as it changed the name
		fileKey2 := fileKey(s3Prefix, topic, 0, 100)
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey2, 900)

		// check that the old file was removed
		_, err = os.Stat(filepath.Join(workingDir, fileKey1))
		require.ErrorIs(t, err, os.ErrNotExist, "old file should not exist anymore")

		// write more records
		writeRecords(t, ctx, adminClient, topic, 0, 1000, 1000)

		// wait until the new local file has the new records
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey2, 1900)
		stopApp(ctx, t, cancel, errCh)
	})

	t.Run("remove leftover partitions folders", func(t *testing.T) {
		t.Parallel()

		topic1 := "delete-leftover-initial-topic"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic1)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("delete-leftover-paritition-folders")
		s3Prefix := "delete-leftover-paritition-folders/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            topic1,
			ExcludeTopicsRegex:     "multiple-.*", // exclude the topics from the previous test
			GroupID:                groupID,
			PartitionIdleThreshold: 1 * time.Second,
			MinFileSize:            100 * 1024 * 1024, // use a big limit, so we make sure no flush occurs
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		firstRunCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(firstRunCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// write records, but offsets won't be committed, since the file size limit is very high
		writeRecords(t, ctx, adminClient, topic1, 0, 1000, 1000)

		fileKey1 := fileKey(s3Prefix, topic1, 0, 0)
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey1, 1000)

		stopApp(ctx, t, cancel, errCh)

		topic2 := "delete-leftover-second-topic"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic2)
		require.NoError(t, err)

		writeRecords(t, ctx, adminClient, topic2, 0, 1000, 1000)

		// consume another topic
		secondRunCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// start the backup again to consume the second topic.
		// The former partitions folder from topic1 should be deleted once the new partitions are assigned
		cfg.TopicsRegex = topic2
		errCh = make(chan error, 1)
		go func() {
			errCh <- Run(secondRunCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)

		// check that the folder of topic1 partition 0 was removed
		_, err = os.Stat(filepath.Dir(filepath.Join(workingDir, fileKey1)))
		require.ErrorIs(t, err, os.ErrNotExist, "old file should not exist anymore")

		stopApp(ctx, t, cancel, errCh)
	})

	t.Run("pause and resume local files", func(t *testing.T) {
		t.Parallel()

		topic4 := "pause-resume-1"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic4)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("test-backup-group")
		s3Prefix := "pause-resume/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            "pause-resume-.*",
			ExcludeTopicsRegex:     "multiple-.*", // exclude the topics from the previous test
			GroupID:                groupID,
			PartitionIdleThreshold: 100 * time.Millisecond, // check for idle partitions very frequently
			MinFileSize:            100 * 1024 * 1024,      // use a big limit, so the flush doesn't happen
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// Run backup with a cancellable context (no timeout, we'll cancel after second batch)
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)
		// write records, but offsets won't be committed, since the file size limit is very high
		writeRecords(t, ctx, adminClient, topic4, 0, 10, 1000)
		fileKey := fileKey(cfg.S3Prefix, topic4, 0, 0)

		waitLocalFileHasRecords(t, ctx, workingDir, fileKey, 10)

		//  Wait for the partition writer to go idle and be paused
		time.Sleep(1 * time.Second)

		// write second batch
		writeRecords(t, ctx, adminClient, topic4, 0, 10, 1000)
		// local file should be resumed
		waitLocalFileHasRecords(t, ctx, workingDir, fileKey, 20)

		// stop consuming
		stopApp(ctx, t, cancel, errCh)
	})

	t.Run("unexpected leftover file", func(t *testing.T) {
		t.Parallel()

		// rig the meter provider, so we can check the counter value
		origProvider := otel.GetMeterProvider()
		defer func() { otel.SetMeterProvider(origProvider) }()

		reader := metric.NewManualReader()
		otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))

		initialValue := readCounterValue(t, reader, "kafka-data-keep", "kafka.data-keep.unexpected-leftover-files")

		topic := "unexpected-leftover-file"
		_, err = kadmClient.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err)

		// Setup backup application config
		workingDir := t.TempDir()

		groupID := newRandomName("unexpected-leftover-file")
		s3Prefix := "unexpected-leftover-file/"
		cfg := AppConfig{
			Brokers:                kafkaBrokers,
			TopicsRegex:            topic,
			ExcludeTopicsRegex:     "multiple-.*",
			GroupID:                groupID,
			PartitionIdleThreshold: 1 * time.Second,
			MinFileSize:            1, // use a small limit
			WorkingDir:             workingDir,
			S3Bucket:               bucketName,
			S3Prefix:               s3Prefix,
			S3Endpoint:             s3Endpoint,
			S3Region:               testutil.MinioRegion,
		}

		// write 150 records
		writeRecords(t, ctx, adminClient, topic, 0, 150, 1000)

		// delete the first 100 records
		delMap := make(kadm.Offsets)
		delMap.Add(kadm.Offset{Topic: topic, Partition: 0, At: 100})
		_, err = kadmClient.DeleteRecords(ctx, delMap)
		require.NoError(t, err)

		// Create a local avro file with zero entries corresponding to the zero offset
		leftoverFileKey := fileKey(s3Prefix, topic, 0, 0)
		leftoverLocalFile := filepath.Join(workingDir, leftoverFileKey)

		createEmptyAvroFile(t, leftoverLocalFile)

		// Run backup with a cancellable context
		backupCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run backup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- Run(backupCtx, cfg)
		}()

		waitConsumerStart(ctx, t, kadmClient, groupID)

		// The backup finds the local file starting at 0, that is less than the current offset (100).
		// Since it's not in S3, it should just log a warning message and increase the counter

		waitForGroupOffsets(t, ctx, kadmClient, groupID, map[string]int{topic: 150})
		stopApp(ctx, t, cancel, errCh)

		_, err := os.Stat(leftoverLocalFile)
		require.NoError(t, err, "the unexpected local file should exist")

		updatedValue := readCounterValue(t, reader, "kafka-data-keep", "kafka.data-keep.unexpected-leftover-files")
		require.Equal(t, initialValue+1, updatedValue, "Counter should have been incremented")
	})
}

// Helper to navigate the OTel data tree
func readCounterValue(t *testing.T, reader *metric.ManualReader, scopeName string, metricName string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name == scopeName {
			for _, m := range sm.Metrics {
				if m.Name == metricName {
					if data, ok := m.Data.(metricdata.Sum[int64]); ok {
						if len(data.DataPoints) > 0 {
							return data.DataPoints[0].Value
						}
					}
				}
			}
		}
	}
	return 0
}

func createEmptyAvroFile(t *testing.T, file string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o755))
	f, err := os.Create(file)
	require.NoError(t, err)

	// Create a valid empty Avro file
	encFactory := &avro.RecordEncoderFactory{}
	enc, err := encFactory.New(f)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	require.NoError(t, f.Close())
}

func waitLocalFileHasRecords(t *testing.T, ctx context.Context, dir string, fileKey string, howMany int) {
	t.Helper()
	filePath := filepath.Join(dir, fileKey)
	timeoutC := time.After(5 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("local file %s did not have expected records after 5 seconds", filePath)
			return
		case <-time.Tick(200 * time.Millisecond):
			// check the file only if it exists
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				t.Logf("Local file %s does not exist yet", filePath)
				continue
			}
			f, err := os.Open(filePath)
			require.NoError(t, err)
			recs := decodeAvroFile(t, f)
			t.Logf("Local file %s has %d records. Expected %d", fileKey, len(recs), howMany)
			if len(recs) == howMany {
				return
			}
		}
	}
}

func fileKey(s3Prefix string, topic4 string, partition int, offset int) string {
	filename := fmt.Sprintf("%s-%d-%s.avro", topic4, partition, fmt.Sprintf("%019d", offset))
	fileKey := filepath.Join(s3Prefix, topic4, fmt.Sprintf("%d", partition), filename)
	return fileKey
}

func stopApp(ctx context.Context, t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()
	cancel()
	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		require.NoError(t, err, "backup returned unexpected error")
	case <-time.After(10 * time.Second):
		t.Fatal("backup did not finish after cancellation")
	}
}

func waitConsumerStart(ctx context.Context, t *testing.T, client *kadm.Client, groupId string) {
	t.Helper()
	timeoutC := time.After(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("consumer group %s did not start in 10 seconds", groupId)
		case <-time.Tick(100 * time.Millisecond):
			dg, err := client.DescribeGroups(ctx, groupId)
			require.NoError(t, err)
			if dg[groupId].State == "Stable" {
				t.Logf("consumer group %s started consuming", groupId)
				return
			}
		}
	}
}

func newRandomName(baseName string) string {
	return baseName + "-" + uuid.NewString()
}

func listFilesOnBucket(ctx context.Context, t *testing.T, s3Client *s3.Client, s3prefix string) map[string]int {
	t.Helper()
	filesFound := make(map[string]int)
	// List files in S3
	listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName), Prefix: aws.String(s3prefix)})
	require.NoError(t, err)

	t.Logf("Found %d files in S3", len(listResp.Contents))

	for _, obj := range listResp.Contents {
		key := *obj.Key

		getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		records := decodeAvroFile(t, getResp.Body)
		filesFound[key] = len(records)
		t.Logf("Found file: %s (size: %d), recs: %d", key, obj.Size, len(records))
	}
	return filesFound
}

// Helper to wait for consumer group offsets
func waitForGroupOffsets(t *testing.T, ctx context.Context, client *kadm.Client, group string, expected map[string]int) {
	t.Helper()
	timeoutC := time.After(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutC:
			t.Fatalf("consumer group %s did not reach expected offsets: %+v", group, expected)
			return
		case <-time.Tick(100 * time.Millisecond):
			if isGroupAt(t, ctx, client, group, expected) {
				return
			}
		}
	}
}

func isGroupAt(t *testing.T, ctx context.Context, client *kadm.Client, group string, expected map[string]int) bool {
	t.Helper()
	topics := make([]string, 0, len(expected))
	for t := range expected {
		topics = append(topics, t)
	}

	offsets, err := client.FetchOffsetsForTopics(ctx, group, topics...)
	require.NoError(t, err)

	for topic, exp := range expected {
		topicOffsets, hasTopicOffset := offsets[topic]
		if !hasTopicOffset {
			t.Logf("Topic %s: no offsets found", topic)
			return false
		}

		currentOffset := getOffsetForTopic(topicOffsets)
		t.Logf("Topic %s: current offset sum: %d, expected: %d", topic, currentOffset, exp)
		if currentOffset < exp {
			return false
		}
	}

	return true
}

func getOffsetForTopic(topicOffsets map[int32]kadm.OffsetResponse) int {
	// Sum offsets for all partitions of the topic
	currentOffsetSum := 0
	for _, pOff := range topicOffsets {
		if pOff.At > 0 {
			currentOffsetSum += int(pOff.At)
		}
	}
	return currentOffsetSum
}

func writeRecords(t *testing.T, ctx context.Context, client *kgo.Client, topic string, partition int32, count int, totalBytes int64) {
	t.Helper()
	recs := make([]*kgo.Record, 0, count)
	for i := range count {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       fmt.Appendf(nil, "key-%s-p%d-%d", topic, partition, i),
			Value:     genBytes(t, totalBytes/int64(count)),
			Headers: []kgo.RecordHeader{
				{Key: "test-header", Value: fmt.Appendf(nil, "header-value-%d", i)},
			},
		}
		recs = append(recs, rec)
	}
	require.NoError(t, client.ProduceSync(ctx, recs...).FirstErr(), "failed to produce records")
}

func genBytes(t *testing.T, size int64) []byte {
	t.Helper()
	data := make([]byte, size)
	// This fills the slice with high-entropy random bits that should not be very compressable
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func decodeAvroFile(t *testing.T, r io.ReadCloser) []*kgo.Record {
	t.Helper()
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Failed to close Avro file reader: %v", err)
		}
	}()
	decFactory := &avro.RecordDecoderFactory{}
	decoder, err := decFactory.New(r)
	require.NoError(t, err)

	records := make([]*kgo.Record, 0, 1000)
	for decoder.HasNext() {
		rec, err := decoder.Decode()
		if err != nil {
			return nil
		}
		records = append(records, rec)
	}

	return records
}
