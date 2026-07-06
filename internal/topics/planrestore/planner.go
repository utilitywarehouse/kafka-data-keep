package planrestore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
)

const (
	// FileIndexHeader is the 1-based index of this file within its partition (absolute across resumes).
	FileIndexHeader = "plan-restore.file-index"
	// TotalFilesHeader is the total number of backup files for this partition.
	TotalFilesHeader = "plan-restore.total-files"
)

type topicInfo struct {
	// sizeBytes is the total size of all S3 objects for this topic.
	sizeBytes int64
	// partitionCounts maps partition name to the number of backup files in that partition.
	partitionCounts map[string]int
}

type planner struct {
	s3Client     *s3.Client
	kafkaClient  *kgo.Client
	latestReader *kafka.LatestReader
	cfg          AppConfig
}

func (p *planner) Run(ctx context.Context) error {
	topics, info, err := p.listTopicsFromS3(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics from S3: %w", err)
	}

	topics, err = p.filterTopics(topics)
	if err != nil {
		return err
	}

	if p.cfg.ProcessLargeTopicsLast {
		thresholdBytes := p.cfg.LargeTopicThresholdMB * 1024 * 1024
		topics = reorderLargeTopicsLast(ctx, topics, info, thresholdBytes)
	}

	if len(topics) == 0 {
		slog.WarnContext(ctx, "No topics found to restore")
		return nil
	}

	slog.InfoContext(ctx, "Planning restore for topics", "count", len(topics), "topics", topics)
	latestRecords, err := p.latestReader.Read(ctx, p.cfg.PlanTopic)
	if err != nil {
		return fmt.Errorf("failed to read latest records from plan topic: %w", err)
	}

	resumeTopic, resumeFile, err := computeResume(latestRecords, topics)
	if err != nil {
		return fmt.Errorf("failed determining resume file: %w", err)
	}

	slog.InfoContext(ctx, "Resuming from file", "file", resumeFile, "topic", resumeTopic)

	resumed := resumeTopic == ""

	for _, topic := range topics {
		// start processing when we reach the resume topic
		if topic == resumeTopic {
			resumed = true
		}

		if resumed {
			// pass the resume file only for the resume topic
			if topic != resumeTopic {
				resumeFile = ""
			}

			if err := p.planForTopic(ctx, topic, resumeFile, info[topic]); err != nil {
				return err
			}
			continue
		}

		slog.InfoContext(ctx, "Skipping topic", "topic", topic)
	}
	return nil
}

func computeResume(latestRecords map[int32]*kgo.Record, topicsOrder []string) (string, string, error) {
	// we might have files from different topics as the last entries in the plan topic
	resumeMap := make(map[string]string)
	for _, rec := range latestRecords {
		file := string(rec.Value)
		topic, _, err := TopicPartitionFromFileName(file)
		if err != nil {
			return "", "", fmt.Errorf("failed to extract topic from file %s: %w", file, err)
		}
		currentVal, exists := resumeMap[topic]
		if !exists {
			resumeMap[topic] = file
		} else if currentVal < file {
			resumeMap[topic] = file
		}
	}

	var lastTopic string
	// taking the last file from the last topic based on the passed in order
	for _, topic := range topicsOrder {
		if _, exists := resumeMap[topic]; exists {
			lastTopic = topic
		}
	}

	if lastTopic == "" {
		return "", "", nil
	}

	return lastTopic, resumeMap[lastTopic], nil
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

// listTopicsFromS3 lists all topics under the configured S3 prefix and aggregates
// per-topic size and per-partition file counts. Topics are returned in first-seen
// lexicographic order (identical to the previous CommonPrefixes listing).
func (p *planner) listTopicsFromS3(ctx context.Context) ([]string, map[string]*topicInfo, error) {
	prefix := p.cfg.S3Prefix
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.cfg.S3.Bucket),
		Prefix: aws.String(prefix),
	}

	var topicsOrdered []string
	infoMap := make(map[string]*topicInfo)

	paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list objects pages: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			topic, partition, err := TopicPartitionFromFileName(*obj.Key)
			if err != nil {
				slog.DebugContext(ctx, "Skipping unparseable S3 key during inventory", "key", *obj.Key, "error", err)
				continue
			}

			ti, exists := infoMap[topic]
			if !exists {
				ti = &topicInfo{partitionCounts: make(map[string]int)}
				infoMap[topic] = ti
				topicsOrdered = append(topicsOrdered, topic)
			}
			if obj.Size != nil {
				ti.sizeBytes += *obj.Size
			}
			ti.partitionCounts[partition]++
		}
	}
	return topicsOrdered, infoMap, nil
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

func (p *planner) planForTopic(ctx context.Context, topic string, resumeFile string, ti *topicInfo) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.cfg.S3.Bucket),
		Prefix: aws.String(p.cfg.S3Prefix + "/" + topic + "/"),
	}

	paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)

	// partIndex tracks the 1-based absolute file index per partition across all pages.
	partIndex := make(map[string]int)

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

			// Reproduce StartAfter semantics: skip keys that were already planned.
			if resumeFile != "" && key <= resumeFile {
				continue
			}

			slog.DebugContext(ctx, "Processing key for topic", "key", key, "topic", topic)

			recs = append(recs, &kgo.Record{
				Value: []byte(key),
				Key:   []byte(partitioningKey(key)),
				Headers: []kgo.RecordHeader{
					{Key: FileIndexHeader, Value: []byte(strconv.Itoa(partIndex[partition]))},
					{Key: TotalFilesHeader, Value: []byte(strconv.Itoa(ti.partitionCounts[partition]))},
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
