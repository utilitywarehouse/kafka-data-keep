package restore

import (
	"context"
	"strconv"

	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Status values recorded by restoreStatusGauge for a consumer group offset partition.
const (
	statusScheduled       int64 = 0
	statusInProgress      int64 = 1
	statusSkipped         int64 = 2
	statusAlreadyRestored int64 = 3
	statusRestored        int64 = 4
)

var restoreStatusGauge = internal.MustInitInt64Gauge(
	"kafka.data-keep.consumergroups-restore.status",
	"Restore status for a consumer group offset partition: 0=scheduled, 1=in_progress, 2=skipped, 3=already_restored, 4=restored",
)

// recordStatusMetric records the restore status for a single consumer group offset partition.
func recordStatusMetric(ctx context.Context, group, topic string, partition int32, status int64) {
	restoreStatusGauge.Record(ctx, status, metric.WithAttributes(
		attribute.String("group", group),
		attribute.String("topic", topic),
		attribute.String("partition", strconv.Itoa(int(partition))),
	))
}
