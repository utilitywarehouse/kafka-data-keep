package backup

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/uwos-go/pubsub/kafka"
	"log/slog"
	"syscall"
)

func runConsumer(ctx context.Context, client *kafka.Client, pwManager *PartitionsWriterManager) error {
	for {
		fetches := client.PollMaxRecords(ctx)
		err, stopProcessing := handleFetches(ctx, &fetches)
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

// These functions are copied over from the uwos-go kafka package as they deal also with reconnections.

func handleFetches(ctx context.Context, fetches *kgo.Fetches) (err error, stopProcessing bool) {
	// Err0 is a quicker way to check for common errors.
	err = fetches.Err0()
	switch {
	// if the context is Done while polling it will exit with an error
	case errors.Is(err, context.Canceled):
		slog.InfoContext(ctx, "stop processing due to context cancelled")
		return nil, true
	case errors.Is(err, kgo.ErrClientClosed):
		slog.InfoContext(ctx, "stop processing due to client closed")
		return nil, true
	case isBrokerGone(err):
		/*	when a broker goes down it should be safe to keep retrying as the metadata refresh was triggered. See https://github.com/twmb/franz-go/issues/784
			we don't need to implement a backoff with this retry at this point, as the loop calls the poll method that will not fail continuously with this error  */
		slog.WarnContext(ctx, "retry fetching due to partitions leader down", slog.Any("error", err))
		return nil, false
	}

	if errs := fetches.Errors(); len(errs) > 0 {
		return fmt.Errorf("failed when fetching from kafka. errors: %v", errs), true
	}
	return nil, false
}

func isBrokerGone(err error) bool {
	/*	check if the error is a kgo.ErrGroupSession resulted from a connection refused source */
	var egs *kgo.ErrGroupSession
	return errors.As(err, &egs) && errors.Is(err, syscall.ECONNREFUSED)
}
