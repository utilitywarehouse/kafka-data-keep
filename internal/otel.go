package internal

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// MustInitInt64Gauge creates an Int64Gauge instrument on the shared "kafka-data-keep" meter.
// It panics if the gauge cannot be created, since that indicates a programming error
// (e.g. an invalid instrument name) rather than a runtime condition to recover from.
func MustInitInt64Gauge(name, desc string) metric.Int64Gauge {
	g, err := otel.Meter("kafka-data-keep").Int64Gauge(name, metric.WithDescription(desc))
	if err != nil {
		panic(fmt.Sprintf("failed to create gauge metric %s: %v", name, err))
	}
	return g
}
