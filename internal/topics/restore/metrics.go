package restore

import (
	"context"

	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var restoreFileIndexGauge = internal.InitInt64Gauge(
	"kafka.data-keep.restore.partition-file-index",
	"1-based index of the backup file currently being restored for a topic partition",
)

// recordFileIndexMetric records the 1-based index of the backup file currently being restored for a topic partition.
func recordFileIndexMetric(ctx context.Context, topicsSHA, topic, partition string, idx int64) {
	restoreFileIndexGauge.Record(ctx, idx, metric.WithAttributes(
		attribute.String("topics_sha", topicsSHA),
		attribute.String("topic", topic),
		attribute.String("partition", partition),
	))
}
