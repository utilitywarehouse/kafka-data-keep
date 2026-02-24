package restore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/codec/avro"
	"github.com/utilitywarehouse/kafka-data-keep/internal/consumergroups/restore"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
	topicsrestore "github.com/utilitywarehouse/kafka-data-keep/internal/topics/restore"
)

func init() {
	slog.SetDefault(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
}

const (
	cgRestoreBucketName   = "cg-restore-test-bucket"
	cgRestoreGroupsPrefix = "restored-"
)

// partitionPlan holds the pre-decided write parameters for one partition.
// These are generated before any messages are written so that backup offsets can be
// chosen (and the Avro file uploaded) before the restore process starts.
type partitionPlan struct {
	topic            string
	partition        int32
	baseSourceOffset int // first restore.source-offset value that will appear on this partition
	msgCount         int // number of messages that will be written
}

// planPartitionWrite returns a randomly-parameterised write plan for the given topic/partition.
func planPartitionWrite(topic string, partition int32) partitionPlan {
	return partitionPlan{
		topic:            topic,
		partition:        partition,
		baseSourceOffset: randomInt(50, 500),
		msgCount:         randomInt(100, 1000),
	}
}

// pickBackupOffset picks a source offset that falls within a plan's written range.
func pickBackupOffset(p partitionPlan) int64 {
	return int64(p.baseSourceOffset + randomInt(1, p.msgCount-2))
}

func TestConsumerGroupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()
	ctx := t.Context()

	// ── Infrastructure ────────────────────────────────────────────────────────
	kafkaBrokers, tkf := testutil.StartKafkaService(t)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := testutil.StartS3Service(t)
	t.Cleanup(ts3f)

	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(t, s3Endpoint)
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cgRestoreBucketName)})
	require.NoError(t, err)

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(producerClient.Close)

	kadmClient := kadm.NewClient(producerClient)
	t.Cleanup(kadmClient.Close)

	t.Run("restore multiple groups", func(t *testing.T) {
		t.Parallel()

		topic1 := randomName("test-multiple-1")
		topic2 := randomName("test-multiple-2")

		// Create two topics with two partitions each.
		_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic1)
		require.NoError(t, err)
		_, err = kadmClient.CreateTopic(ctx, 2, 1, nil, topic2)
		require.NoError(t, err)

		// ── Step 1: Pre-plan writes so we know the source-offset ranges up front
		plans := map[string]partitionPlan{
			partKey(topic1, 0): planPartitionWrite(topic1, 0),
			partKey(topic1, 1): planPartitionWrite(topic1, 1),
			partKey(topic2, 0): planPartitionWrite(topic2, 0),
			partKey(topic2, 1): planPartitionWrite(topic2, 1),
		}

		// ── Step 2: Pick backup offsets within each partition's planned range
		group1ID := randomName("test-multiple-1")
		group2ID := randomName("test-multiple-2")
		// this group is backed up, but will be ignored
		ignoreGroupId := "ignore-group-2"

		group1Offsets := make(map[string]int64, len(plans))
		group2Offsets := make(map[string]int64, len(plans))
		for key, plan := range plans {
			group1Offsets[key] = pickBackupOffset(plan)
			group2Offsets[key] = pickBackupOffset(plan)
		}

		backupGroups := []codec.ConsumerGroupOffset{
			{
				GroupID: group1ID,
				Topics: []codec.TopicOffset{
					{
						Topic: topic1,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: group1Offsets[partKey(topic1, 0)]},
							{Partition: 1, Offset: group1Offsets[partKey(topic1, 1)]},
						},
					},
					{
						Topic: topic2,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: group1Offsets[partKey(topic2, 0)]},
							{Partition: 1, Offset: group1Offsets[partKey(topic2, 1)]},
						},
					},
				},
			},
			{
				GroupID: group2ID,
				Topics: []codec.TopicOffset{
					{
						Topic: topic1,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: group2Offsets[partKey(topic1, 0)]},
							{Partition: 1, Offset: group2Offsets[partKey(topic1, 1)]},
						},
					},
					{
						Topic: topic2,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: group2Offsets[partKey(topic2, 0)]},
							{Partition: 1, Offset: group2Offsets[partKey(topic2, 1)]},
						},
					},
				},
			},
			{
				GroupID: ignoreGroupId,
				Topics: []codec.TopicOffset{
					{
						Topic: topic1,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: 1},
							{Partition: 1, Offset: 2},
						},
					},
				},
			},
		}

		// Encode groups to Avro and upload to S3
		s3Location := writeBackupToS3(t, s3Client, cgRestoreBucketName, backupGroups)

		// ── Step 3: Start the restore process in the background ───────────────────
		// RestoreTopicsPrefix is empty: the topics created above are the restored topics directly.
		// RestoreGroupsPrefix is "restored-": committed group IDs will be "restored-<original>".
		restoreCfg := restore.AppConfig{
			Brokers:             kafkaBrokers,
			S3Bucket:            cgRestoreBucketName,
			S3Region:            testutil.MinioRegion,
			S3Endpoint:          s3Endpoint,
			S3Location:          s3Location,
			RestoreGroupsPrefix: cgRestoreGroupsPrefix,
			RestoreTopicsPrefix: "", // topics are not prefixed
			IncludeRegexes:      "test-multiple.*",
			LoopInterval:        50 * time.Millisecond,
		}

		restoreCtx, restoreCancel := context.WithTimeout(ctx, 60*time.Second)
		defer restoreCancel()

		restoreErrCh := make(chan error, 1)
		go func() {
			restoreErrCh <- restore.Run(restoreCtx, restoreCfg)
		}()

		// ── Step 4: Write messages to all partitions ───────────────────────────────
		// The restore loop will keep polling until it sees records with source offsets
		// that satisfy all backup offsets, so writing after the restore starts is fine.
		for _, plan := range plans {
			writePartition(t, producerClient, plan)
		}

		// ── Step 5: Wait for the restore process to finish ────────────────────────
		select {
		case err := <-restoreErrCh:
			require.NoError(t, err, "consumer group restore returned an unexpected error")
		case <-restoreCtx.Done():
			t.Fatal("restore did not finish within the deadline")
		}

		// ── Step 6: Validate restored offsets ─────────────────────────────────────
		// For a partition whose Kafka offsets start at 0 and whose first source offset is
		// baseSourceOffset, the record with source offset S lives at Kafka offset (S - baseSourceOffset).
		// Committed offsets represent "next to read", so the expected committed value is
		// (backupOffset - baseSourceOffset)
		restoredGroup1 := cgRestoreGroupsPrefix + group1ID
		restoredGroup2 := cgRestoreGroupsPrefix + group2ID

		for _, p := range plans {
			verifyRestoredGroupOffset(t, kadmClient, restoredGroup1, group1Offsets[partKey(p.topic, p.partition)], p)
			verifyRestoredGroupOffset(t, kadmClient, restoredGroup2, group2Offsets[partKey(p.topic, p.partition)], p)
		}

		// check that the ignore group was ignored
		fetched, err := kadmClient.FetchOffsets(ctx, ignoreGroupId)
		require.NoError(t, err)
		require.Empty(t, fetched, "The group that was expected to be ignored was included")

		t.Log("TestConsumerGroupRestore finished successfully")
	})
}

// writePartition writes the messages described by p to Kafka.
// Each message carries a `restore.source-offset` header with value p.baseSourceOffset+i,
// simulating records that were previously restored from another cluster.
func writePartition(t *testing.T, client *kgo.Client, p partitionPlan) {
	t.Helper()
	recs := make([]*kgo.Record, 0, p.msgCount)
	for i := range p.msgCount {
		rec := &kgo.Record{
			Topic:     p.topic,
			Partition: p.partition,
			Key:       fmt.Appendf(nil, "key-%s-p%d-%d", p.topic, p.partition, i),
			Value:     fmt.Appendf(nil, "value-%s-p%d-%d", p.topic, p.partition, i),
			// Temporarily set Offset to the desired source offset so that
			// SetOriginalOffsetHeader stamps the correct value.
			Offset: int64(p.baseSourceOffset + i),
		}
		topicsrestore.SetOriginalOffsetHeader(rec)
		rec.Offset = 0 // Kafka assigns the actual offset on produce
		recs = append(recs, rec)
	}
	require.NoError(t, client.ProduceSync(t.Context(), recs...).FirstErr(), "failed to produce records")
	t.Logf("wrote %d messages to %s/%d with baseSourceOffset=%d", p.msgCount, p.topic, p.partition, p.baseSourceOffset)
}

// writeBackupToS3 encodes the groups to avro and uploads to a random location.
func writeBackupToS3(t *testing.T, s3Client *s3.Client, bucket string, groups []codec.ConsumerGroupOffset) string {
	t.Helper()
	var buf bytes.Buffer
	factory := &avro.GroupEncoderFactory{}
	enc, err := factory.New(&buf)
	require.NoError(t, err)
	for i := range groups {
		require.NoError(t, enc.Encode(&groups[i]))
	}
	require.NoError(t, enc.Flush())
	require.NoError(t, enc.Close())

	key := randomName("backup/cg-bacup") + ".avro"
	_, err = s3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	require.NoError(t, err)
	return key
}

// verifyRestoredGroupOffset fetches the committed offset for the restored group on the given
// topic/partition and asserts it corresponds to the record whose restore.source-offset header
// equals backupOffset.
// Because records are written sequentially starting at Kafka offset 0 with source offsets
// beginning at p.baseSourceOffset, the Kafka offset of the record with source offset S is
// S - baseSourceOffset.
func verifyRestoredGroupOffset(t *testing.T, kadmClient *kadm.Client, group string, backupOffset int64, p partitionPlan) {
	t.Helper()
	ctx := t.Context()

	fetched, err := kadmClient.FetchOffsets(ctx, group)
	require.NoError(t, err)

	topicOffsets, ok := fetched[p.topic]
	require.True(t, ok, "group %s: no offsets found for topic %s", group, p.topic)

	partOffset, ok := topicOffsets[p.partition]
	require.True(t, ok, "group %s: no offsets found for topic %s partition %d", group, p.topic, p.partition)

	expectedCommitted := backupOffset - int64(p.baseSourceOffset)
	require.Equal(t, expectedCommitted, partOffset.At,
		"group %s topic %s partition %d: committed offset mismatch (backup offset=%d, baseSourceOffset=%d)",
		group, p.topic, p.partition, backupOffset, p.baseSourceOffset,
	)
}

func randomInt(minVal, maxVal int) int {
	bigInt, err := rand.Int(rand.Reader, big.NewInt(int64(maxVal-minVal+1)))
	if err != nil {
		panic(err)
	}
	return int(bigInt.Int64()) + minVal
}

func randomName(baseName string) string {
	return baseName + "-" + uuid.NewString()
}

func partKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}
