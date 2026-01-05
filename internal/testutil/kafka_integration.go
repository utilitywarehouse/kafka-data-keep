package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

func StartKafkaService(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()
	redpandaContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v25.1.1")
	require.NoError(t, err)
	terminateFunc := func() {
		if err := redpandaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Redpanda container: %v", err)
		}
	}

	kafkaBrokers, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return kafkaBrokers, terminateFunc
}
