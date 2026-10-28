package eventqueue

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

const KVMaxBytes = kvMaxBytes

// MaxStore returns the JetStream file storage limit of the embedded server.
func (eq *EventQueue) MaxStore() int64 {
	return eq.server.JetStreamConfig().MaxStore
}

// StreamMessages returns the number of messages currently stored in the
// events stream.
func (eq *EventQueue) StreamMessages(ctx context.Context) (uint64, error) {
	info, err := eq.jetStreamStream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.Msgs, nil
}

// IsProcessed reports whether the event was recorded in the persistent
// deduplication store.
func (eq *EventQueue) IsProcessed(ctx context.Context, event *corev1.Event) bool {
	_, err := eq.keyValue.Get(ctx, eventIdentifier(event))
	return err == nil
}
