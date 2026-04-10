package eventsender

import "context"

// EventSender defines the interface that all event senders must implement.
// It provides a common contract for sending Kubernetes events to various
// backends (e.g., Loki, etc.).
type EventSender interface {
	// Name returns the unique name of this sender instance.
	Name() string

	// Run starts the sender loop, fetching events and sending them to the backend.
	// It blocks until the context is cancelled or Stop is called.
	Run(ctx context.Context) error

	// Stop signals the sender to stop processing and clean up resources.
	Stop()
}
