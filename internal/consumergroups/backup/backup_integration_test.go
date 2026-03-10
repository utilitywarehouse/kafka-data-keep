package backup

import (
	"context"
	"fmt"
	"sort"
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
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
	"github.com/utilitywarehouse/kafka-data-keep/internal/testutil"
)

const (
	bucketName = "test-consumer-groups-backup-bucket"
)

func TestBackupIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaBrokers, tkf := testutil.StartKafkaService(t)
	t.Cleanup(tkf)

	s3Endpoint, ts3f := testutil.StartS3Service(t)
	t.Cleanup(ts3f)

	testutil.SetupEnvS3Access()
	s3Client := testutil.NewS3Client(t, s3Endpoint)

	_, err := s3Client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // set the partitions manually on produce
	)
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)

	kadmClient := kadm.NewClient(adminClient)
	t.Cleanup(kadmClient.Close)

	ctx := t.Context()

	topic1 := "test-topic-1"
	topic2 := "test-topic-2"
	partitions := int32(2)
	_, err = kadmClient.CreateTopic(ctx, partitions, 1, nil, topic1)
	require.NoError(t, err)
	_, err = kadmClient.CreateTopic(ctx, partitions, 1, nil, topic2)
	require.NoError(t, err)

	writeRecords(t, adminClient, topic1, 0, 10)
	writeRecords(t, adminClient, topic1, 1, 30)
	writeRecords(t, adminClient, topic2, 0, 40)
	writeRecords(t, adminClient, topic2, 1, 50)

	groupID := "test-group-" + uuid.NewString()

	// set the group to the end of the topic
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topic1, Partition: 0, At: 5, LeaderEpoch: -1})
	offsets.Add(kadm.Offset{Topic: topic1, Partition: 1, At: 20, LeaderEpoch: -1})
	offsets.Add(kadm.Offset{Topic: topic2, Partition: 0, At: 30, LeaderEpoch: -1})
	offsets.Add(kadm.Offset{Topic: topic2, Partition: 1, At: 40, LeaderEpoch: -1})

	_, err = kadmClient.CommitOffsets(ctx, groupID, offsets)
	require.NoError(t, err)

	// 3. Run Backup
	s3Location := "backups/consumer-groups.avro"
	runInterval := 500 * time.Millisecond

	cfg := AppConfig{
		KafkaConfig: kafka.Config{
			Brokers: kafkaBrokers,
		},
		S3: ints3.Config{
			Bucket:   bucketName,
			Region:   testutil.MinioRegion,
			Endpoint: s3Endpoint,
		},
		S3Location:  s3Location,
		RunInterval: runInterval,
	}

	appCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(appCtx, cfg)
	}()

	// Wait for backup to run and upload file
	require.Eventually(t, func() bool {
		// Check if file exists
		_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Location),
		})
		return err == nil
	}, 10*time.Second, 500*time.Millisecond, "Failed to find backup file in S3")

	stopApp(t, cancel, errCh)

	// 4. Download and Decode
	getObj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Location),
	})
	require.NoError(t, err)
	defer func() {
		_ = getObj.Body.Close()
	}()

	decFactory := &avro.GroupDecoderFactory{}
	decoder, err := decFactory.New(getObj.Body)
	require.NoError(t, err)

	// 5. Verify Content
	require.True(t, decoder.HasNext())
	decodedOffsets, err := decoder.Decode()
	require.NoError(t, err)

	// Sort topics and partitions for deterministic comparison
	sort.Slice(decodedOffsets.Topics, func(i, j int) bool {
		return decodedOffsets.Topics[i].Topic < decodedOffsets.Topics[j].Topic
	})
	for _, tOffset := range decodedOffsets.Topics {
		sort.Slice(tOffset.Partitions, func(i, j int) bool {
			return tOffset.Partitions[i].Partition < tOffset.Partitions[j].Partition
		})
	}

	expectedOffsets := &codec.ConsumerGroupOffset{
		GroupID: groupID,
		Topics: []codec.TopicOffset{
			{
				Topic: topic1,
				Partitions: []codec.PartitionOffset{
					{Partition: 0, Offset: 5},
					{Partition: 1, Offset: 20},
				},
			},
			{
				Topic: topic2,
				Partitions: []codec.PartitionOffset{
					{Partition: 0, Offset: 30},
					{Partition: 1, Offset: 40},
				},
			},
		},
	}
	require.Equal(t, expectedOffsets, decodedOffsets)
}

func stopApp(t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()
	cancel()
	select {
	case <-t.Context().Done():
		return
	case err := <-errCh:
		require.NoError(t, err, "backup returned unexpected error")
	case <-time.After(10 * time.Second):
		t.Fatal("backup did not finish after cancellation")
	}
}

func writeRecords(t *testing.T, client *kgo.Client, topic string, partition int32, count int) {
	t.Helper()
	recs := make([]*kgo.Record, 0, count)
	for i := range count {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       fmt.Appendf(nil, "key-%s-p%d-%d", topic, partition, i),
			Value:     fmt.Appendf(nil, "value-%s-p%d-%d", topic, partition, i),
		}
		recs = append(recs, rec)
	}
	require.NoError(t, client.ProduceSync(t.Context(), recs...).FirstErr(), "failed to produce records")
}
