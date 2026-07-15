package planrestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
)

const (
	// PartitionFileIndexHeader is the 1-based index of this file within its partition (absolute across resumes).
	PartitionFileIndexHeader = "plan-restore.partition-file-index"
	// PartitionTotalFilesHeader is the total number of backup files for this partition.
	PartitionTotalFilesHeader = "plan-restore.partition-total-files"
	// TopicTotalFilesHeader is the total number of backup files for this topic, summed across all its partitions.
	TopicTotalFilesHeader = "plan-restore.topic-total-files"
	// TopicsSHAHeader is the SHA-256 of the ordered topic list at plan time.
	// Used to detect topic-set changes between a prior plan run and a resume.
	TopicsSHAHeader = "plan-restore.topics-sha"
)

type topicInfo struct {
	// sizeBytes is the total size of all S3 objects for this topic.
	sizeBytes int64
	// partitionCounts maps partition name to the number of backup files in that partition.
	partitionCounts map[string]int
}

// totalFiles returns the total number of backup files for this topic, summed across all its partitions.
func (ti *topicInfo) totalFiles() int {
	total := 0
	for _, count := range ti.partitionCounts {
		total += count
	}
	return total
}

type planner struct {
	s3Client     *s3.Client
	kafkaClient  *kgo.Client
	latestReader *kafka.LatestReader
	cfg          AppConfig
}

func (p *planner) Run(ctx context.Context) error {
	topics, err := p.listTopicsFromS3(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics from S3: %w", err)
	}

	// filter topics based on include/exclude regexes, and only afterwards build the topics info to avoid listing S3 objects for excluded topics
	topics, err = p.filterTopics(topics)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		slog.WarnContext(ctx, "No topics found to restore")
		return nil
	}

	info, err := p.buildTopicsInfo(ctx, topics)
	if err != nil {
		return fmt.Errorf("failed to get topics info from S3: %w", err)
	}

	if p.cfg.ProcessLargeTopicsLast {
		thresholdBytes := p.cfg.LargeTopicThresholdMB * 1024 * 1024
		topics = reorderLargeTopicsLast(ctx, topics, info, thresholdBytes)
	}

	sha := computeTopicsSHA(topics)

	slog.InfoContext(ctx, "Planning restore for topics", "count", len(topics), "topics", topics, "sha", sha)

	latestRecords, err := p.latestReader.Read(ctx, p.cfg.PlanTopic)
	if err != nil {
		return fmt.Errorf("failed to read latest records from plan topic: %w", err)
	}

	rs, err := computeResume(latestRecords, topics)
	if err != nil {
		return fmt.Errorf("failed determining resume file: %w", err)
	}

	if rs != nil && rs.topicsSHA != sha {
		return fmt.Errorf("topics SHA mismatch: the prior plan run used a different set of topics (recorded SHA %s, current SHA %s); empty the plan topic to run from scratch or align the topic configuration", rs.topicsSHA, sha)
	}

	slog.InfoContext(ctx, "Computed resume state", "resume_state", rs)

	resumed := rs == nil

	for _, topic := range topics {
		// start processing when we reach the resume topic
		if rs != nil && topic == rs.topic {
			resumed = true
		}

		if !resumed {
			slog.InfoContext(ctx, "Skipping topic", "topic", topic)
			continue
		}

		if err := p.planForTopic(ctx, topic, rs, info[topic], sha); err != nil {
			return err
		}
	}
	return nil
}

type resumeState struct {
	topic     string
	file      string
	fileIndex int    // 1-based absolute index from PartitionFileIndexHeader; 0 when absent
	topicsSHA string // SHA-256 of the topic list from TopicsSHAHeader; "" when absent (old records)
}

func computeResume(latestRecords map[int32]*kgo.Record, topicsOrder []string) (*resumeState, error) {
	if len(latestRecords) == 0 || len(topicsOrder) == 0 {
		return nil, nil
	}

	// we might have files from different topics as the last entries in the plan topic
	resumeMap := make(map[string]*kgo.Record)
	for _, rec := range latestRecords {
		file := string(rec.Value)
		topic, _, err := TopicPartitionFromFileName(file)
		if err != nil {
			return nil, fmt.Errorf("failed to extract topic from file %s: %w", file, err)
		}
		current, exists := resumeMap[topic]
		if !exists {
			resumeMap[topic] = rec
		} else if string(current.Value) < file {
			resumeMap[topic] = rec
		}
	}

	var lastTopic string
	// taking the last file from the last topic based on the passed in order
	for _, topic := range topicsOrder {
		if _, exists := resumeMap[topic]; exists {
			lastTopic = topic
		}
	}

	// the last topic is not included in the list of topics
	if lastTopic == "" {
		return nil, fmt.Errorf("failed computing resume state: the prior plan used a different set of topics (topics in latest records: %v, current topics: %v); empty the plan topic to run from scratch or align the topic configuration", slices.Collect(maps.Keys(resumeMap)), topicsOrder)
	}

	resumeRec := resumeMap[lastTopic]
	rs := resumeState{
		topic: lastTopic,
		file:  string(resumeRec.Value),
	}
	for _, h := range resumeRec.Headers {
		switch h.Key {
		case PartitionFileIndexHeader:
			if idx, err := strconv.Atoi(string(h.Value)); err == nil {
				rs.fileIndex = idx
			}
		case TopicsSHAHeader:
			rs.topicsSHA = string(h.Value)
		}
	}

	return &rs, nil
}

func (p *planner) filterTopics(topics []string) ([]string, error) {
	includeRegexes, err := internal.CompileRegexes(p.cfg.RestoreTopicsRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid include regex: %w", err)
	}

	excludeRegexes, err := internal.CompileRegexes(p.cfg.ExcludeTopicsRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid exclude regex: %w", err)
	}

	var result []string
	included := make(map[string]struct{}, len(topics))
	/* we want the result to be ordered by the included topics regex list */
	for _, includeRegex := range includeRegexes {
		for _, topic := range topics {
			/* include the topic if it matches the regex and doesn't match any of the exclude regexes'*/
			if includeRegex.MatchString(topic) && !internal.MatchesAny(topic, excludeRegexes) {
				_, alreadyIncluded := included[topic]
				if !alreadyIncluded {
					result = append(result, topic)
					included[topic] = struct{}{}
				}
			}
		}
	}

	return result, nil
}

// listTopicsFromS3 lists the topic names under the configured S3 prefix, using a
// delimited listing so only the topic-level "directories" are returned (no object
// bodies/sizes are fetched). Topics are returned in lexicographic order.
func (p *planner) listTopicsFromS3(ctx context.Context) ([]string, error) {
	slog.InfoContext(ctx, "Listing topics from S3 ...")
	prefix := normalizePrefix(p.cfg.S3Prefix)

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(p.cfg.S3.Bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	var topics []string

	paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects pages: %w", err)
		}

		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			topic := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, prefix), "/")
			if topic == "" {
				continue
			}
			topics = append(topics, topic)
		}
	}
	return topics, nil
}

// buildTopicsInfo lists the S3 objects for each of the given topics and aggregates
// per-topic size and per-partition file counts.
func (p *planner) buildTopicsInfo(ctx context.Context, topics []string) (map[string]*topicInfo, error) {
	slog.InfoContext(ctx, "Building topics info from S3 ...")
	infoMap := make(map[string]*topicInfo, len(topics))

	for _, topic := range topics {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(p.cfg.S3.Bucket),
			Prefix: aws.String(normalizePrefix(p.cfg.S3Prefix) + topic + "/"),
		}

		ti := &topicInfo{partitionCounts: make(map[string]int)}

		paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to list objects for topic %s: %w", topic, err)
			}

			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}
				_, partition, err := TopicPartitionFromFileName(*obj.Key)
				if err != nil {
					slog.WarnContext(ctx, "Skipping unparseable S3 key during inventory", "key", *obj.Key, "error", err)
					continue
				}
				if obj.Size != nil {
					ti.sizeBytes += *obj.Size
				}
				ti.partitionCounts[partition]++
			}
		}

		infoMap[topic] = ti
	}

	return infoMap, nil
}

// normalizePrefix ensures the S3 prefix ends with a single trailing slash.
func normalizePrefix(prefix string) string {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

// reorderLargeTopicsLast moves topics whose total size exceeds thresholdBytes to the
// end of the list, preserving relative order within each group.
func reorderLargeTopicsLast(ctx context.Context, topics []string, info map[string]*topicInfo, thresholdBytes int64) []string {
	var small, large []string
	for _, topic := range topics {
		ti := info[topic]
		if ti != nil && ti.sizeBytes > thresholdBytes {
			slog.InfoContext(ctx, "Deferring large topic to end of plan", "topic", topic, "size_bytes", ti.sizeBytes, "threshold_bytes", thresholdBytes)
			large = append(large, topic)
		} else {
			small = append(small, topic)
		}
	}
	return append(small, large...)
}

func (p *planner) planForTopic(ctx context.Context, topic string, rs *resumeState, ti *topicInfo, topicsSHA string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.cfg.S3.Bucket),
		Prefix: aws.String(normalizePrefix(p.cfg.S3Prefix) + topic + "/"),
	}

	// partIndex tracks the 1-based absolute file index per partition across all pages.
	// For the resume partition, seed from the stored header so the next file gets the
	// correct absolute index without re-listing files before StartAfter.
	partIndex := make(map[string]int)
	topicTotalFiles := strconv.Itoa(ti.totalFiles())

	if rs != nil && rs.topic == topic {
		input.StartAfter = aws.String(rs.file)

		_, resumePartition, err := TopicPartitionFromFileName(rs.file)
		if err != nil {
			return fmt.Errorf("failed to extract resume partition from file %s: %w", rs.file, err)
		}

		partIndex[resumePartition] = rs.fileIndex
	}

	paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get page: %w", err)
		}

		recs := make([]*kgo.Record, 0, len(page.Contents))
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key

			_, partition, err := TopicPartitionFromFileName(key)
			if err != nil {
				slog.WarnContext(ctx, "Skipping unparseable key in planForTopic", "key", key, "error", err)
				continue
			}

			partIndex[partition]++

			slog.DebugContext(ctx, "Processing key for topic", "key", key, "topic", topic)

			recs = append(recs, &kgo.Record{
				Value: []byte(key),
				Key:   []byte(partitioningKey(key)),
				Headers: []kgo.RecordHeader{
					{Key: PartitionFileIndexHeader, Value: []byte(strconv.Itoa(partIndex[partition]))},
					{Key: PartitionTotalFilesHeader, Value: []byte(strconv.Itoa(ti.partitionCounts[partition]))},
					{Key: TopicTotalFilesHeader, Value: []byte(topicTotalFiles)},
					{Key: TopicsSHAHeader, Value: []byte(topicsSHA)},
				},
			})
		}

		if len(recs) == 0 {
			continue
		}

		if res := p.kafkaClient.ProduceSync(ctx, recs...); res.FirstErr() != nil {
			return fmt.Errorf("failed to produce records for topic %s: %w", topic, res.FirstErr())
		}
	}
	slog.InfoContext(ctx, "Finished processing topic", "topic", topic)

	return nil
}

func computeTopicsSHA(topics []string) string {
	h := sha256.New()
	h.Write([]byte(strings.Join(topics, ",")))
	return hex.EncodeToString(h.Sum(nil))
}

func partitioningKey(path string) string {
	// keep the prefix part without the trailing file part, to get all the files for a topic in the same partition, to process them in order
	if idx := strings.LastIndexByte(path, '/'); idx >= 0 {
		return path[:idx+1]
	}
	return path
}

func TopicPartitionFromFileName(fileName string) (string, string, error) {
	if fileName == "" {
		return "", "", nil
	}
	// from a key like kafka-backup/account-identity.account.change.events/7/account-identity.account.change.events-7-0000000000000000000.avro we want to extract the topic
	parts := strings.Split(fileName, "/")

	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid file name %s. Expected in the format folder/topic/partition/filename.avro", fileName)
	}
	return parts[len(parts)-3], parts[len(parts)-2], nil
}
