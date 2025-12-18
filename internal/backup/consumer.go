package backup

import (
	"context"
	"fmt"

	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
)

func runConsumer(ctx context.Context, client *kafka.Client, pwManager *PartitionsWriterManager) error {
	for {
		fetches := client.PollMaxRecords(ctx)

		err, stopProcessing := kafkaint.HandleFetches(ctx, &fetches)
		if stopProcessing || err != nil {
			return err
		}

		for _, fetch := range fetches {
			for _, ft := range fetch.Topics {
				for _, fp := range ft.Partitions {
					w, err := pwManager.GetWriter(ft.Topic, fp.Partition)
					if err != nil {
						return fmt.Errorf("unexpected state: %w", err)
					}

					if err := w.WriteRecords(ctx, fp.Records); err != nil {
						return fmt.Errorf("error writing records for topic %s and partition %d: %w", ft.Topic, fp.Partition, err)
					}
				}
			}
		}
		client.AllowRebalance()
	}
}
