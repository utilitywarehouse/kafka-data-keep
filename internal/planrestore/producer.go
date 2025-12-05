package planrestore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
	"log/slog"
	"strings"
)

type Planner struct {
	s3Client *s3.Client
	producer *kafka.Client
	cfg      AppConfig
}

func (p *Planner) Run(ctx context.Context) error {
	for _, topic := range strings.Split(p.cfg.RestoreTopics, ",") {
		if err := p.planForTopic(ctx, strings.TrimSpace(topic)); err != nil {
			return err
		}
	}
	return nil
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
