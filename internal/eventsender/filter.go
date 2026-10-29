package eventsender

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	corev1 "k8s.io/api/core/v1"

	"github.com/ninech/kigeon/internal/eventqueue"
	"github.com/ninech/kigeon/internal/metrics"
)

// EventFetcher fetches events from the event queue.
type EventFetcher interface {
	Fetch(ctx context.Context) (*corev1.Event, eventqueue.EventAcknowledger, error)
}

// ObjectFilter decides whether events of an involved object get sent.
type ObjectFilter interface {
	IsAllowedForObject(obj corev1.ObjectReference) bool
}

// FilteredFetcher returns an EventFetcher of the given sender which
// acknowledges and skips events whose involved object is rejected by the
// filter.
func FilteredFetcher(sender string, fetcher EventFetcher, filter ObjectFilter, logger *slog.Logger) EventFetcher {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &filteredFetcher{sender: sender, fetcher: fetcher, filter: filter, logger: logger}
}

type filteredFetcher struct {
	sender  string
	fetcher EventFetcher
	filter  ObjectFilter
	logger  *slog.Logger
}

func (f *filteredFetcher) Fetch(ctx context.Context) (*corev1.Event, eventqueue.EventAcknowledger, error) {
	for {
		event, ack, err := f.fetcher.Fetch(ctx)
		if err != nil {
			return nil, nil, err
		}
		if f.filter.IsAllowedForObject(event.InvolvedObject) {
			return event, ack, nil
		}

		f.logger.Debug("namespace filter skipped event",
			slog.String("namespace", event.InvolvedObject.Namespace),
			slog.String("kind", event.InvolvedObject.Kind),
			slog.String("reason", event.Reason),
		)
		if err := ack.Ack(); err != nil {
			return nil, nil, fmt.Errorf("failed to acknowledge filtered event: %w", err)
		}
		metrics.SenderEvents.WithLabelValues(f.sender, metrics.ResultFiltered).Inc()
	}
}
