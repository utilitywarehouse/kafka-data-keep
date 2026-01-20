package restore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/backup"
	"github.com/utilitywarehouse/kafka-data-keep/internal/planrestore"
	"github.com/utilitywarehouse/kafka-data-keep/internal/restore"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
	"math/rand"
)

func TestRestoreE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx := context.Background()

	// 1. Start Services
	kafkaBrokers, tkf := testutil.StartKafkaService(ctx, t)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := testutil.StartS3Service(ctx, t)
	t.Cleanup(ts3f)

	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(ctx, t, s3Endpoint)
	bucketName := "e2e-bucket"
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
	)
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)
	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	// 2. Create source topic with 15 partitions
	srcTopic := "e2e-source-topic"
	partitions := 15
	_, err = kadmClient.CreateTopic(ctx, int32(partitions), 1, nil, srcTopic)
	require.NoError(t, err)

	// Start Backup
	backupGroup := newRandomName("e2e-backup")
	workingDir := t.TempDir()
	s3Prefix := "backup-data"

	backupCfg := backup.AppConfig{
		Brokers:                kafkaBrokers,
		TopicsRegex:            srcTopic,
		GroupID:                backupGroup,
		MinFileSize:            1, // minimum file size to force flush and commit after every batch read from kafka
		PartitionIdleThreshold: 200 * time.Millisecond,
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

	testutil.WaitConsumerStart(ctx, t, kadmClient, backupGroup)

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer producerClient.Close()

	totalRecsPerPartition := writeSequencedRecords(t, ctx, producerClient, srcTopic, partitions, 10)
	totalRecords := total(totalRecsPerPartition)
	testutil.WaitForGroupOffsets(t, ctx, kadmClient, backupGroup, map[string]int{srcTopic: totalRecords})

	stopApp(ctx, t, backupCancel, backupErrCh)

	// Create Plan Topic (15 partitions)
	planTopic := "e2e-plan-topic"
	_, err = kadmClient.CreateTopic(ctx, int32(partitions), 1, nil, planTopic)
	require.NoError(t, err)

	// Run Plan Restore
	planCfg := planrestore.AppConfig{
		Brokers:            kafkaBrokers,
		PlanTopic:          planTopic,
		S3Bucket:           bucketName,
		S3Prefix:           s3Prefix,
		S3Endpoint:         s3Endpoint,
		S3Region:           testutil.MinioRegion,
		RestoreTopicsRegex: srcTopic, // Restore our source topic
	}

	err = planrestore.Run(ctx, planCfg)
	require.NoError(t, err)

	// Create Restore Topic (15 partitions) with "restored" prefix
	restoredTopic := "restored-" + srcTopic
	_, err = kadmClient.CreateTopic(ctx, int32(partitions), 1, nil, restoredTopic)
	require.NoError(t, err)

	// 8. Run Restore
	restoreGroup := newRandomName("e2e-restore")
	restoreCfg := restore.AppConfig{
		Brokers:            kafkaBrokers,
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

	restoredRecs, err := testutil.WaitForRecords(t, ctx, restoredTopic, kafkaBrokers, totalRecords)
	require.NoError(t, err)

	stopApp(ctx, t, restoreCancel, restoreErrCh)

	// Check distribution and content
	counts := make(map[int32]int)
	recsByPartition := make(map[int32][]*kgo.Record)

	for _, r := range restoredRecs {
		counts[r.Partition]++
		recsByPartition[r.Partition] = append(recsByPartition[r.Partition], r)
	}

	require.Equal(t, partitions, len(counts), "Should have messages in all partitions")
	for p := 0; p < partitions; p++ {
		require.Equal(t, totalRecsPerPartition[p], counts[int32(p)], "Partition %d count mismatch", p)

		// check ordering and content
		pRecs := recsByPartition[int32(p)]

		// We expect strict order per partition
		currentIdx := 0
		for _, r := range pRecs {
			expectedVal := fmt.Sprintf("val-%d", currentIdx)
			expectedKey := fmt.Sprintf("key-%d", p)
			require.Equal(t, expectedKey, string(r.Key), "Key mismatch at partition %d index %d", p, currentIdx)
			require.Equal(t, expectedVal, string(r.Value), "Value mismatch at partition %d index %d", p, currentIdx)
			currentIdx++
		}
	}
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

func writeSequencedRecords(t *testing.T, ctx context.Context, client *kgo.Client, topic string, partitions int, loops int) map[int]int {
	t.Helper()
	totalRecsPerPartition := make(map[int]int)

	// produce records in loops, each time writing a different number of records per partition
	for range loops {
		recs := make([]*kgo.Record, 0)
		for p := range partitions {
			msgsPerPartition := randomInt(100, 1000)
			for i := 0; i < msgsPerPartition; i++ {
				val := totalRecsPerPartition[p] + i
				rec := &kgo.Record{
					Topic:     topic,
					Partition: int32(p),
					Key:       []byte(fmt.Sprintf("key-%d", p)),
					Value:     []byte(fmt.Sprintf("val-%d", val)),
					Headers: []kgo.RecordHeader{
						{Key: "test-header", Value: fmt.Appendf(nil, "header-value-%d", val)},
					},
				}
				recs = append(recs, rec)
			}
			totalRecsPerPartition[p] += msgsPerPartition

		}

		require.NoError(t, client.ProduceSync(ctx, recs...).FirstErr(), "failed to produce records")
		require.NoError(t, client.Flush(ctx))
		// give the consumer time to consume the records
		time.Sleep(100 * time.Millisecond)
	}

	return totalRecsPerPartition
}

func randomInt(min, max int) int {
	return rand.Intn(max-min+1) + min
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
