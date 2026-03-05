package backup

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	kafkaint "github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
)

const maxPollRecords = 10000 // this affects how many records are processed per poll, not how many are fetched from Kafka
func runConsumer(ctx context.Context, client *kgo.Client, pwManager *partitionsWriterManager) error {
	for {
		fetches := client.PollRecords(ctx, maxPollRecords)

		stopProcessing, err := kafkaint.HandleFetches(ctx, &fetches)
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
