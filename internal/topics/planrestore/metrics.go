package planrestore

import (
	"context"

	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	planPartitionTotalFilesGauge = internal.MustInitInt64Gauge(
		"kafka.data-keep.plan-restore.partition-total-files",
		"Total number of backup files for a topic partition, as planned",
	)
	planTopicProgressGauge = internal.MustInitInt64Gauge(
		"kafka.data-keep.plan-restore.topic-progress",
		"Progress of planning a topic: 0 while pending, 1 once it has been planned or skipped on resume",
	)
)

// recordPartitionTotalFilesMetric records the total number of planned backup files for a topic partition.
func recordPartitionTotalFilesMetric(ctx context.Context, topicsSHA, topic, partition string, total int) {
	planPartitionTotalFilesGauge.Record(ctx, int64(total), metric.WithAttributes(
		attribute.String("topics_sha", topicsSHA),
		attribute.String("topic", topic),
		attribute.String("partition", partition),
	))
}

// recordTopicProgressMetric records whether a topic has been planned (or skipped on resume) yet.
func recordTopicProgressMetric(ctx context.Context, topicsSHA, topic string, done bool) {
	var val int64
	if done {
		val = 1
	}
	planTopicProgressGauge.Record(ctx, val, metric.WithAttributes(
		attribute.String("topics_sha", topicsSHA),
		attribute.String("topic", topic),
	))
}
