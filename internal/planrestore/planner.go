package planrestore

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

type Planner struct {
	s3Client *s3.Client
	producer *kafka.Client
	cfg      AppConfig
}

func (p *Planner) Run(ctx context.Context) error {
	topics, err := p.listTopicsFromS3(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics from S3: %w", err)
	}

	topics, err = p.filterTopics(topics)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		slog.WarnContext(ctx, "No topics found to restore")
		return nil
	}

	slog.InfoContext(ctx, "Planning restore for topics", "count", len(topics), "topics", topics)

	for _, topic := range topics {
		if err := p.planForTopic(ctx, topic); err != nil {
			return err
		}
	}
	return nil
}

func (p *Planner) filterTopics(topics []string) ([]string, error) {
	includeRegexes, err := compileRegexes(p.cfg.RestoreTopicsRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid include regex: %w", err)
	}

	excludeRegexes, err := compileRegexes(p.cfg.ExcludeTopicsRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid exclude regex: %w", err)
	}

	var result []string
	/* we want the result to be ordered by the included topics regex list */
	for _, includeRegex := range includeRegexes {
		for _, topic := range topics {
			/* include the topic if it matches the regex and doesn't match any of the exclude regexes'*/
			if includeRegex.MatchString(topic) && !matchesAny(topic, excludeRegexes) {
				result = append(result, topic)
			}
		}
	}

	return result, nil
}

func matchesAny(s string, regexes []*regexp.Regexp) bool {
	for _, re := range regexes {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func (p *Planner) listTopicsFromS3(ctx context.Context) ([]string, error) {
	prefix := p.cfg.S3Prefix
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(p.cfg.S3Bucket),
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

		for _, commonPrefix := range page.CommonPrefixes {
			if commonPrefix.Prefix == nil {
				continue
			}
			// commonPrefix is like "prefix/topic/"
			// we want "topic"
			// remove prefix
			cp := *commonPrefix.Prefix
			if strings.HasPrefix(cp, prefix) {
				sub := cp[len(prefix):]
				// remove trailing slash
				topic := strings.TrimSuffix(sub, "/")
				if topic != "" {
					topics = append(topics, topic)
				}
			}
		}
	}
	return topics, nil
}

func (p *Planner) planForTopic(ctx context.Context, topic string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.cfg.S3Bucket),
		Prefix: aws.String(p.cfg.S3Prefix + "/" + topic + "/"),
	}

	paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get page: %w", err)
		}

		// 4. Process the page contents
		recs := make([]*kgo.Record, 0, len(page.Contents))
		for _, obj := range page.Contents {
			if obj.Key != nil {
				slog.InfoContext(ctx, "Processing key for topic", "key", *obj.Key, "topic", topic)
				recs = append(recs, &kgo.Record{
					Value: []byte(*obj.Key),
					Key:   []byte(partitioningKey(*obj.Key)),
				})
			}
		}
		if res := p.producer.ProduceSync(ctx, recs...); res.FirstErr() != nil {
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

func compileRegexes(regexStr string) ([]*regexp.Regexp, error) {
	var regexes []*regexp.Regexp
	if regexStr != "" {
		for r := range strings.SplitSeq(regexStr, ",") {
			re, err := regexp.Compile(strings.TrimSpace(r))
			if err != nil {
				return nil, fmt.Errorf("invalid regex '%s': %w", r, err)
			}
			regexes = append(regexes, re)
		}
	}
	return regexes, nil
}
