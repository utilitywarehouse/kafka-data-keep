package planrestore

import (
	"context"

	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	planPartitionTotalFilesGauge = internal.InitInt64Gauge(
		"kafka.data-keep.plan-restore.partition-total-files",
		"Total number of backup files for a topic partition, as planned",
	)
	planTopicsTotalGauge = internal.InitInt64Gauge(
		"kafka.data-keep.plan-restore.topics-total",
		"Total number of topics included in the current restore plan",
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

// recordTopicsTotalMetric records the number of topics included in the current restore plan.
func recordTopicsTotalMetric(ctx context.Context, topicsSHA string, total int) {
	planTopicsTotalGauge.Record(ctx, int64(total), metric.WithAttributes(
		attribute.String("topics_sha", topicsSHA),
	))
}
