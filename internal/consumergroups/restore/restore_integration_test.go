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
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
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
	cgRestoreTopicPrefix  = "restore."
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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
		_, err := kadmClient.CreateTopic(ctx, 2, 1, nil, topic1)
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
		ignoreGroupID := "ignore-group-2"

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
				GroupID: ignoreGroupID,
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
			{
				GroupID: "empty-group",
				// this should be skipped as it doesn't have any partitions saved
				Topics: []codec.TopicOffset{
					{
						Topic: topic1,
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
			KafkaConfig: kafka.Config{
				Brokers: kafkaBrokers,
			},
			S3: ints3.Config{
				Bucket:   cgRestoreBucketName,
				Region:   testutil.MinioRegion,
				Endpoint: s3Endpoint,
			},
			S3Location:          s3Location,
			RestoreGroupsPrefix: cgRestoreGroupsPrefix,
			RestoreTopicsPrefix: "", // topics are not prefixed
			IncludeRegexes:      ".*",
			ExcludeRegexes:      ignoreGroupID, // exclude everything not included
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
		fetched, err := kadmClient.FetchOffsets(ctx, ignoreGroupID)
		require.NoError(t, err)
		require.Empty(t, fetched, "The group that was expected to be ignored was included")

		t.Log("TestConsumerGroupRestore finished successfully")
	})

	t.Run("skip already restored", func(t *testing.T) {
		t.Parallel()

		topic := randomName("test-skip")
		restoredTopic := cgRestoreTopicPrefix + topic

		// Create a single topic
		_, err := kadmClient.CreateTopic(ctx, 2, 1, nil, restoredTopic)
		require.NoError(t, err)

		// write messages to this topic
		p0Plan := partitionPlan{topic: restoredTopic, partition: 0, msgCount: 100}
		writePartition(t, producerClient, p0Plan)
		p1Plan := partitionPlan{topic: restoredTopic, partition: 1, msgCount: 200}
		writePartition(t, producerClient, p1Plan)

		// create the consumer group with offsets, so that it's already restored
		group := randomName("test-skip-restored")
		restoredGroup := cgRestoreGroupsPrefix + group

		offsets := make(kadm.Offsets)
		offsets.Add(kadm.Offset{Topic: restoredTopic, Partition: 0, At: 50})
		offsets.Add(kadm.Offset{Topic: restoredTopic, Partition: 1, At: 60})
		resp, err := kadmClient.CommitOffsets(ctx, restoredGroup, offsets)
		require.NoError(t, err)
		require.NoError(t, resp.Error())

		backupGroups := []codec.ConsumerGroupOffset{
			{
				GroupID: group,
				Topics: []codec.TopicOffset{
					{
						Topic: topic,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: 80},
							{Partition: 1, Offset: 110},
						},
					},
				},
			},
		}

		// Encode groups to Avro and upload to S3
		s3Location := writeBackupToS3(t, s3Client, cgRestoreBucketName, backupGroups)

		restoreCfg := restore.AppConfig{
			KafkaConfig: kafka.Config{
				Brokers: kafkaBrokers,
			},
			S3: ints3.Config{
				Bucket:   cgRestoreBucketName,
				Region:   testutil.MinioRegion,
				Endpoint: s3Endpoint,
			},
			S3Location:          s3Location,
			RestoreGroupsPrefix: cgRestoreGroupsPrefix,
			RestoreTopicsPrefix: cgRestoreTopicPrefix,
			IncludeRegexes:      ".*",
			LoopInterval:        50 * time.Millisecond,
		}

		// restore should finish right away, as the group should be skipped as it has offsets
		restoreCtx, cancelFunc := context.WithTimeout(ctx, 4*time.Second)
		defer cancelFunc()
		err = restore.Run(restoreCtx, restoreCfg)
		require.NoError(t, err)

		// check that the offsets were not advanced
		verifyRestoredGroupOffset(t, kadmClient, restoredGroup, 50, p0Plan)
		verifyRestoredGroupOffset(t, kadmClient, restoredGroup, 60, p1Plan)
	})

	t.Run("target offset after the expected one", func(t *testing.T) {
		t.Parallel()

		topic := randomName("test-offset-after")

		// Create a single topic
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err)

		// write messages to this topic
		plan := partitionPlan{topic: topic, partition: 0, baseSourceOffset: 10, msgCount: 100}
		writePartition(t, producerClient, plan)

		backupOffset := int64(50)
		// write the last record so that the diff between its offset and the source offset will be greater than 10, the base source offset used.
		// This will cause that on restore, the offset of the consumer group (50) will be first looked up at 50 - (115-100) = 35, and then the app will look further and find it on 40
		lastRec := &kgo.Record{Topic: topic, Partition: 0, Offset: 115}
		topicsrestore.SetOriginalOffsetHeader(lastRec)
		producerClient.ProduceSync(ctx, lastRec)

		// create the consumer group with offsets, so that it's already restored
		groupID := randomName("test-offset-after")

		backupGroups := []codec.ConsumerGroupOffset{
			{
				GroupID: groupID,
				Topics: []codec.TopicOffset{
					{
						Topic: topic,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: backupOffset},
						},
					},
				},
			},
		}

		// Encode groups to Avro and upload to S3
		s3Location := writeBackupToS3(t, s3Client, cgRestoreBucketName, backupGroups)

		restoreCfg := restore.AppConfig{
			KafkaConfig: kafka.Config{
				Brokers: kafkaBrokers,
			},
			S3: ints3.Config{
				Bucket:   cgRestoreBucketName,
				Region:   testutil.MinioRegion,
				Endpoint: s3Endpoint,
			},
			S3Location:          s3Location,
			RestoreGroupsPrefix: "",
			RestoreTopicsPrefix: "", // topics are not prefixed
			IncludeRegexes:      ".*",
			LoopInterval:        50 * time.Millisecond,
		}

		// restore should finish right away, as the messages are already written
		restoreCtx, cancelFunc := context.WithTimeout(ctx, 4*time.Second)
		defer cancelFunc()
		err = restore.Run(restoreCtx, restoreCfg)
		require.NoError(t, err)

		verifyRestoredGroupOffset(t, kadmClient, groupID, backupOffset, plan)
	})

	t.Run("target offset before the expected one", func(t *testing.T) {
		t.Parallel()

		//	test with a restore topic prefix
		restoreTopicPrefix := "restore."
		topic := randomName("test-offset-before")
		restoreTopicName := restoreTopicPrefix + topic

		// Create a single topic
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, restoreTopicName)
		require.NoError(t, err)

		// write messages to this topic
		plan := partitionPlan{topic: restoreTopicName, partition: 0, baseSourceOffset: 10, msgCount: 100}
		writePartition(t, producerClient, plan)

		backupOffset := int64(50)
		// write the last record so that the diff between its offset and the source offset will be less than 10, the base source offset used.
		// This will cause that on restore, the offset of the consumer group (50) will be first looked up at 50 - (105-100) = 45, and then the app will look backward and find it on 40
		lastRec := &kgo.Record{Topic: restoreTopicName, Partition: 0, Offset: 105}
		topicsrestore.SetOriginalOffsetHeader(lastRec)
		results := producerClient.ProduceSync(ctx, lastRec)
		require.NoError(t, results.FirstErr())

		// create the consumer group with offsets, so that it's already restored
		groupID := randomName("test-offset-before")

		backupGroups := []codec.ConsumerGroupOffset{
			{
				GroupID: groupID,
				Topics: []codec.TopicOffset{
					{
						Topic: topic,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: backupOffset},
						},
					},
				},
			},
		}

		// Encode groups to Avro and upload to S3
		s3Location := writeBackupToS3(t, s3Client, cgRestoreBucketName, backupGroups)

		restoreCfg := restore.AppConfig{
			KafkaConfig: kafka.Config{
				Brokers: kafkaBrokers,
			},
			S3: ints3.Config{
				Bucket:   cgRestoreBucketName,
				Region:   testutil.MinioRegion,
				Endpoint: s3Endpoint,
			},
			S3Location:          s3Location,
			RestoreGroupsPrefix: "",
			RestoreTopicsPrefix: restoreTopicPrefix,
			IncludeRegexes:      ".*",
			LoopInterval:        50 * time.Millisecond,
		}

		// restore should finish right away, as the messages are already written
		restoreCtx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()
		err = restore.Run(restoreCtx, restoreCfg)
		require.NoError(t, err)

		verifyRestoredGroupOffset(t, kadmClient, groupID, backupOffset, plan)
	})

	t.Run("target offset missing from the restored data", func(t *testing.T) {
		t.Parallel()

		//	test with a restore topic prefix
		restoreTopicPrefix := "restore."
		topic := randomName("test-offset-missing")
		restoreTopicName := restoreTopicPrefix + topic

		// Create a single topic
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, restoreTopicName)
		require.NoError(t, err)

		backupOffset := int64(50)
		// write a single record so that the diff between its offset and the source offset will negative,
		// This will cause that on restore, the offset of the consumer group (50) will be first looked up at 50 - (200-0) = -150, which wouldn't exist, so the group should be set at the start of the partition
		lastRec := &kgo.Record{Topic: restoreTopicName, Partition: 0, Offset: 200}
		topicsrestore.SetOriginalOffsetHeader(lastRec)
		results := producerClient.ProduceSync(ctx, lastRec)
		require.NoError(t, results.FirstErr())

		// create the consumer group with offsets, so that it's already restored
		groupID := randomName("test-offset-before")

		backupGroups := []codec.ConsumerGroupOffset{
			{
				GroupID: groupID,
				Topics: []codec.TopicOffset{
					{
						Topic: topic,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: backupOffset},
						},
					},
				},
			},
		}

		// Encode groups to Avro and upload to S3
		s3Location := writeBackupToS3(t, s3Client, cgRestoreBucketName, backupGroups)

		restoreCfg := restore.AppConfig{
			KafkaConfig: kafka.Config{
				Brokers: kafkaBrokers,
			},
			S3: ints3.Config{
				Bucket:   cgRestoreBucketName,
				Region:   testutil.MinioRegion,
				Endpoint: s3Endpoint,
			},
			S3Location:          s3Location,
			RestoreGroupsPrefix: "",
			RestoreTopicsPrefix: restoreTopicPrefix,
			IncludeRegexes:      ".*",
			LoopInterval:        50 * time.Millisecond,
		}

		// restore should finish right away, as the messages are already written
		restoreCtx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()
		err = restore.Run(restoreCtx, restoreCfg)
		require.NoError(t, err)

		// force the computed expected offset to be 0
		expectedPlan := partitionPlan{topic: restoreTopicName, partition: 0, baseSourceOffset: 0}
		verifyRestoredGroupOffset(t, kadmClient, groupID, 0, expectedPlan)
	})

	t.Run("target offset wasn't backed up", func(t *testing.T) {
		t.Parallel()

		topic := randomName("test-offset-not-backed-up")

		// Create a single topic
		_, err := kadmClient.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err)

		// write messages to this topic
		plan := partitionPlan{topic: topic, partition: 0, baseSourceOffset: 10, msgCount: 100}
		writePartition(t, producerClient, plan)

		// set the backup offset to a not existing one
		backupOffset := int64(115)
		// write the last record so that the diff between its offset and the source offset will be greater than 10, the base source offset used.
		// This will cause that on restore, the offset of the consumer group (115) will be first looked up at 115 - (120-100) = 95,
		// and then the app will look further and not find the actual record with source offset 115, so it will settle for the previous one, meaning the last record in the batch, the one with source offset 109 at offset 99
		lastRec := &kgo.Record{Topic: topic, Partition: 0, Offset: 120}
		topicsrestore.SetOriginalOffsetHeader(lastRec)
		producerClient.ProduceSync(ctx, lastRec)

		// create the consumer group with offsets, so that it's already restored
		groupID := randomName("test-offset-not-backed-up")

		backupGroups := []codec.ConsumerGroupOffset{
			{
				GroupID: groupID,
				Topics: []codec.TopicOffset{
					{
						Topic: topic,
						Partitions: []codec.PartitionOffset{
							{Partition: 0, Offset: backupOffset},
						},
					},
				},
			},
		}

		// Encode groups to Avro and upload to S3
		s3Location := writeBackupToS3(t, s3Client, cgRestoreBucketName, backupGroups)

		restoreCfg := restore.AppConfig{
			KafkaConfig: kafka.Config{
				Brokers: kafkaBrokers,
			},
			S3: ints3.Config{
				Bucket:   cgRestoreBucketName,
				Region:   testutil.MinioRegion,
				Endpoint: s3Endpoint,
			},
			S3Location:          s3Location,
			RestoreGroupsPrefix: "",
			RestoreTopicsPrefix: "", // topics are not prefixed
			IncludeRegexes:      ".*",
			LoopInterval:        50 * time.Millisecond,
		}

		// restore should finish right away, as the messages are already written
		restoreCtx, cancelFunc := context.WithTimeout(ctx, 4*time.Second)
		defer cancelFunc()
		err = restore.Run(restoreCtx, restoreCfg)
		require.NoError(t, err)

		verifyRestoredGroupOffset(t, kadmClient, groupID, 109, plan)
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
