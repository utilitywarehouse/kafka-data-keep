package restore_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/backup"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/planrestore"
	"github.com/utilitywarehouse/kafka-data-keep/internal/topics/restore"
)

func init() {
	// Enable debug logging for slog
	slog.SetDefault(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
}

const (
	srcTopic           = "e2e-source-topic"
	srcTopicPartitions = 15
	s3Prefix           = "backup-data"
	planTopic          = "e2e-plan-topic"
	bucketName         = "e2e-bucket"
)

func TestRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	t.Parallel()
	ctx := t.Context()

	// Start Services
	kafkaBrokers, tkf := testutil.StartKafkaService(t)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := testutil.StartS3Service(t)
	t.Cleanup(ts3f)

	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(t, s3Endpoint)
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
	)
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)
	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	totalRecsPerPartition, deletedRecsPerPartition := feedTopicAndRunBackup(t, kadmClient, kafkaBrokers, s3Endpoint)

	// Manually duplicate the file with 0 offset on S3 for partitions.
	// This simulates a scenario where an overlapping or duplicate file exists
	for p := range srcTopicPartitions {
		duplicateRandomFilesForPartition(ctx, t, s3Client, p, randomInt(2, 5))
	}

	runPlanRestore(ctx, t, kadmClient, kafkaBrokers, s3Endpoint)

	// Create the restore Topic (15 partitions) with the "restored" prefix
	restoredTopic := "restored-" + srcTopic
	_, err = kadmClient.CreateTopic(ctx, int32(srcTopicPartitions), 1, nil, restoredTopic)
	require.NoError(t, err)

	// 8. Run Restore
	restoreGroup := newRandomName("e2e-restore")
	restoreCfg := restore.AppConfig{
		KafkaConfig: kafkaint.Config{
			Brokers: kafkaBrokers,
		},
		PlanTopic:          planTopic,
		RestoreTopicPrefix: "restored-",
		ConsumerGroup:      restoreGroup,
		S3Bucket:           bucketName,
		S3Endpoint:         s3Endpoint,
		S3Region:           testutil.MinioRegion,
	}

	restoreCtx, restoreCancel := context.WithCancel(ctx)
	defer restoreCancel()

	restoreErrCh := make(chan error, 1)
	go func() {
		restoreErrCh <- restore.Run(restoreCtx, restoreCfg)
	}()

	// pause the restore after some records were restored. Stopping after processing between 10 and 70 percent.
	_, err = testutil.WaitForRecords(t, restoredTopic, kafkaBrokers, total(totalRecsPerPartition)*randomInt(10, 70)/100)
	require.NoError(t, err)

	stopApp(t, restoreCancel, restoreErrCh)

	// rewind the offsets in the plan topic to the beginning so that we force it to reconsume the messages to check the resume mechanism
	resetConsumerGroup(ctx, t, kadmClient, restoreGroup)

	// start again the restore
	resumeRestoreCtx, resumeRestoreCancel := context.WithCancel(ctx)
	defer resumeRestoreCancel()
	resumeRestoreErrCh := make(chan error, 1)
	go func() {
		resumeRestoreErrCh <- restore.Run(resumeRestoreCtx, restoreCfg)
	}()

	// Wait for the restore group to finish consuming the plan topic
	testutil.WaitConsumeAll(t, kadmClient, planTopic, restoreGroup)

	stopApp(t, resumeRestoreCancel, resumeRestoreErrCh)

	validateRestoredRecords(t, restoredTopic, kafkaBrokers, totalRecsPerPartition, deletedRecsPerPartition)
	t.Log("finished test successfully")
}

func duplicateRandomFilesForPartition(ctx context.Context, t *testing.T, s3Client *s3.Client, partition int, count int) {
	t.Helper()

	files := listS3FilesForPartition(ctx, t, s3Client, partition)

	for range count {
		// Pick random file
		srcKey := files[randomInt(0, len(files)-1)]
		destKey := genRandomDestKey(t, srcKey, partition, files)

		_, err := s3Client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			CopySource: aws.String(bucketName + "/" + srcKey),
			Key:        aws.String(destKey),
		})
		require.NoError(t, err)
		files = append(files, destKey)

		t.Logf("Duplicated %s to %s", srcKey, destKey)
	}
}

func genRandomDestKey(t *testing.T, srcKey string, partition int, files []string) string {
	t.Helper()
	srcOffset := extractOffset(t, srcKey)

	// Find a new offset that doesn't exist
	var destKey string
	for {
		// Ensure new offset is greater than source offset to avoid restoring future records early
		// We add a random delta to the source offset
		newOffset := srcOffset + randomInt(1, 1000)
		destKey = testutil.FileKey(s3Prefix, srcTopic, partition, newOffset)

		if !slices.Contains(files, destKey) {
			return destKey
		}
	}
}

func listS3FilesForPartition(ctx context.Context, t *testing.T, s3Client *s3.Client, partition int) []string {
	t.Helper()
	prefix := filepath.Dir(testutil.FileKey(s3Prefix, srcTopic, partition, 0)) + "/"
	resp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err)

	files := make([]string, 0, len(resp.Contents))
	for _, obj := range resp.Contents {
		files = append(files, *obj.Key)
	}

	require.NotEmpty(t, files, "no files found for partition %d", partition)
	return files
}

func extractOffset(t *testing.T, key string) int {
	t.Helper()
	// key: .../topic-partition-00000.avro
	// we assume format ends with -offset.avro
	base := filepath.Base(key)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	lastDash := strings.LastIndex(name, "-")
	require.NotEqual(t, -1, lastDash, "invalid filename format %s", key)

	offStr := name[lastDash+1:]
	val, err := strconv.Atoi(offStr)
	require.NoError(t, err, "failed to parse offset from %s", key)
	return val
}

func validateRestoredRecords(t *testing.T, restoredTopic string, kafkaBrokers string, expectedRecsPerPartition map[int]int, deletedRecsPerPartition map[int]int) {
	t.Helper()
	// Verify restored records
	restoredRecs, err := testutil.ReadAll(t, restoredTopic, kafkaBrokers)
	require.NoError(t, err)

	// Check distribution and content
	countsByPart := make(map[int]int)
	recsByPart := make(map[int][]*kgo.Record)

	for _, r := range restoredRecs {
		countsByPart[int(r.Partition)]++
		recsByPart[int(r.Partition)] = append(recsByPart[int(r.Partition)], r)
	}

	require.Len(t, countsByPart, srcTopicPartitions, "Should have messages in all partitions")
	for p := range srcTopicPartitions {
		require.Equal(t, expectedRecsPerPartition[p], countsByPart[p], "Partition %d count mismatch", p)

		// check ordering and content
		pRecs := recsByPart[p]

		// We expect strict order per partition
		currentIdx := 0
		for _, r := range pRecs {
			expectedVal := fmt.Sprintf("val-%d", currentIdx)
			expectedKey := fmt.Sprintf("key-%d", p)
			require.Equal(t, expectedKey, string(r.Key), "Key mismatch at partition %d index %d", p, currentIdx)
			require.Equal(t, expectedVal, string(r.Value), "Value mismatch at partition %d index %d", p, currentIdx)

			expectHeader(t, r, "test-header", fmt.Sprintf("header-value-%d", currentIdx))
			expectHeader(t, r, "restore.source-offset", fmt.Sprintf("%d", currentIdx+deletedRecsPerPartition[p]))
			currentIdx++
		}
	}
}

func resetConsumerGroup(ctx context.Context, t *testing.T, kadmClient *kadm.Client, group string) {
	t.Helper()
	resp, err := kadmClient.DeleteGroup(ctx, group)
	require.NoError(t, err)
	require.NoError(t, resp.Err, "Failed to reset consumer group offsets")
}

func runPlanRestore(ctx context.Context, t *testing.T, kadmClient *kadm.Client, kafkaBrokers string, s3Endpoint string) {
	t.Helper()
	// Create the plan Topic (5 partitions)
	_, err := kadmClient.CreateTopic(ctx, 5, 1, nil, planTopic)
	require.NoError(t, err)

	// Run Plan Restore
	planCfg := planrestore.AppConfig{
		KafkaConfig: kafkaint.Config{
			Brokers: kafkaBrokers,
		},
		PlanTopic:          planTopic,
		S3Bucket:           bucketName,
		S3Prefix:           s3Prefix,
		S3Endpoint:         s3Endpoint,
		S3Region:           testutil.MinioRegion,
		RestoreTopicsRegex: srcTopic, // Restore our source topic
	}

	require.NoError(t, planrestore.Run(ctx, planCfg))
}

func feedTopicAndRunBackup(t *testing.T, kadmClient *kadm.Client, kafkaBrokers string, s3Endpoint string) (map[int]int, map[int]int) {
	t.Helper()
	ctx := t.Context()
	// Create the source topic with 15 partitions
	_, err := kadmClient.CreateTopic(ctx, int32(srcTopicPartitions), 1, nil, srcTopic)
	require.NoError(t, err)

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer producerClient.Close()

	// create and delete messages from the topic, to advance the start offset of the topic, so the offsets won't match on restore
	deleteRecordsPerPartition := writeSequencedRecords(t, producerClient, srcTopic, srcTopicPartitions, 1)

	delOffsets := make(kadm.Offsets)
	for p, count := range deleteRecordsPerPartition {
		delOffsets.Add(kadm.Offset{
			Topic: srcTopic,
			//nolint:gosec // partitions count is small enough to fit in int32
			Partition: int32(p),
			At:        int64(count),
		})
	}
	_, err = kadmClient.DeleteRecords(ctx, delOffsets)
	require.NoError(t, err)

	// Start Backup
	backupGroup := newRandomName("e2e-backup")
	workingDir := t.TempDir()

	backupCfg := backup.AppConfig{
		KafkaConfig: kafkaint.Config{
			Brokers: kafkaBrokers,
		},
		TopicsRegex:            srcTopic,
		GroupID:                backupGroup,
		MinFileSize:            1, // minimum file size to force flush and commit after every batch read from kafka
		PartitionIdleThreshold: 50 * time.Millisecond,
		WorkingDir:             workingDir,
		S3Bucket:               bucketName,
		S3Prefix:               s3Prefix,
		S3Endpoint:             s3Endpoint,
		S3Region:               testutil.MinioRegion,
	}

	backupCtx, backupCancel := context.WithCancel(ctx)
	defer backupCancel()

	backupErrCh := make(chan error, 1)
	go func() {
		backupErrCh <- backup.Run(backupCtx, backupCfg)
	}()

	testutil.WaitConsumerStart(t, kadmClient, backupGroup)

	totalRecsPerPartition := writeSequencedRecords(t, producerClient, srcTopic, srcTopicPartitions, 10)
	testutil.WaitConsumeAll(t, kadmClient, srcTopic, backupGroup)

	stopApp(t, backupCancel, backupErrCh)
	return totalRecsPerPartition, deleteRecordsPerPartition
}

func expectHeader(t *testing.T, r *kgo.Record, headerName string, value string) {
	t.Helper()
	for _, h := range r.Headers {
		if h.Key == headerName {
			require.Equal(t, value, string(h.Value), "Header value mismatch")
			return
		}
	}
	t.Fatalf("Header %s not found in record", headerName)
}

func total(valPerPartition map[int]int) int {
	t := 0
	for _, val := range valPerPartition {
		t += val
	}
	return t
}

func newRandomName(baseName string) string {
	return baseName + "-" + uuid.NewString()
}

func writeSequencedRecords(t *testing.T, client *kgo.Client, topic string, partitions int, loops int) map[int]int {
	t.Helper()
	totalRecsPerPartition := make(map[int]int)

	// produce records in loops, each time writing a different number of records per partition
	for range loops {
		recs := make([]*kgo.Record, 0, 100)
		for p := range partitions {
			msgsPerPartition := randomInt(100, 1000)
			for i := range msgsPerPartition {
				val := totalRecsPerPartition[p] + i
				rec := &kgo.Record{
					Topic: topic,
					//nolint:gosec // partitions count is small enough to fit in int32
					Partition: int32(p),
					Key:       fmt.Appendf(nil, "key-%d", p),
					Value:     fmt.Appendf(nil, "val-%d", val),
					Headers: []kgo.RecordHeader{
						{Key: "test-header", Value: fmt.Appendf(nil, "header-value-%d", val)},
					},
				}
				recs = append(recs, rec)
			}
			totalRecsPerPartition[p] += msgsPerPartition
		}

		require.NoError(t, client.ProduceSync(t.Context(), recs...).FirstErr(), "failed to produce records")
		require.NoError(t, client.Flush(t.Context()))
		// give the consumer time to consume the records
		time.Sleep(100 * time.Millisecond)
	}

	return totalRecsPerPartition
}

func randomInt(minVal, maxVal int) int {
	bigInt, err := rand.Int(rand.Reader, big.NewInt(int64(maxVal-minVal+1)))
	if err != nil {
		panic(err)
	}
	return int(bigInt.Int64()) + minVal
}

func stopApp(t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()
	cancel()
	select {
	case <-t.Context().Done():
		return
	case err := <-errCh:
		if errors.Is(err, context.Canceled) {
			return
		}
		require.NoError(t, err, "app returned unexpected error")
	case <-time.After(10 * time.Second):
		t.Fatal("app did not finish after cancellation")
	}
}
