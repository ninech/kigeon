package eventsender_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ninech/kigeon/internal/eventqueue"
	"github.com/ninech/kigeon/internal/eventsender"
)

// fakeFetcher returns the queued events in order and records acknowledgements.
type fakeFetcher struct {
	events []*corev1.Event
	acked  []string
	ackErr error
}

func (f *fakeFetcher) Fetch(_ context.Context) (*corev1.Event, eventqueue.EventAcknowledger, error) {
	if len(f.events) == 0 {
		return nil, nil, eventqueue.ErrNoMessage
	}
	event := f.events[0]
	f.events = f.events[1:]
	return event, ackFunc(func() error {
		if f.ackErr != nil {
			return f.ackErr
		}
		f.acked = append(f.acked, event.Name)
		return nil
	}), nil
}

type ackFunc func() error

func (a ackFunc) Ack() error { return a() }

// namespaceFilter allows the given namespaces and, if set, cluster-scoped objects.
type namespaceFilter struct {
	namespaces           map[string]bool
	includeNonNamespaced bool
}

func (f namespaceFilter) IsAllowedForObject(obj corev1.ObjectReference) bool {
	if obj.Namespace == "" {
		return f.includeNonNamespaced
	}
	return f.namespaces[obj.Namespace]
}

func TestFilteredFetcher(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events               []*corev1.Event
		includeNonNamespaced bool
		wantEvent            string
		wantAcked            []string
	}{
		"allowed event is returned": {
			events:    []*corev1.Event{newEvent("allowed", "project")},
			wantEvent: "allowed",
		},
		"rejected events are acknowledged and skipped": {
			events: []*corev1.Event{
				newEvent("rejected-1", "kube-system"),
				newEvent("rejected-2", "kube-system"),
				newEvent("allowed", "project"),
			},
			wantEvent: "allowed",
			wantAcked: []string{"rejected-1", "rejected-2"},
		},
		"cluster-scoped object is rejected by default": {
			events:    []*corev1.Event{newEvent("node", ""), newEvent("allowed", "project")},
			wantEvent: "allowed",
			wantAcked: []string{"node"},
		},
		"cluster-scoped object is allowed if configured": {
			events:               []*corev1.Event{newEvent("node", "")},
			includeNonNamespaced: true,
			wantEvent:            "node",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fetcher := &fakeFetcher{events: tc.events}
			filtered := eventsender.FilteredFetcher(fetcher, namespaceFilter{
				namespaces:           map[string]bool{"project": true},
				includeNonNamespaced: tc.includeNonNamespaced,
			}, nil)

			event, ack, err := filtered.Fetch(t.Context())
			require.NoError(t, err)
			require.NotNil(t, ack)
			assert.Equal(t, tc.wantEvent, event.Name)
			assert.Equal(t, tc.wantAcked, fetcher.acked)
		})
	}
}

func TestFilteredFetcher_Errors(t *testing.T) {
	t.Parallel()
	filter := namespaceFilter{namespaces: map[string]bool{"project": true}}

	t.Run("fetch errors are returned", func(t *testing.T) {
		t.Parallel()
		filtered := eventsender.FilteredFetcher(&fakeFetcher{}, filter, nil)
		_, _, err := filtered.Fetch(t.Context())
		require.ErrorIs(t, err, eventqueue.ErrNoMessage)
	})

	t.Run("only rejected events return no message", func(t *testing.T) {
		t.Parallel()
		fetcher := &fakeFetcher{events: []*corev1.Event{newEvent("rejected", "kube-system")}}
		filtered := eventsender.FilteredFetcher(fetcher, filter, nil)
		_, _, err := filtered.Fetch(t.Context())
		require.ErrorIs(t, err, eventqueue.ErrNoMessage)
		assert.Equal(t, []string{"rejected"}, fetcher.acked)
	})

	t.Run("acknowledgement errors are returned", func(t *testing.T) {
		t.Parallel()
		ackErr := errors.New("ack failed")
		fetcher := &fakeFetcher{events: []*corev1.Event{newEvent("rejected", "kube-system")}, ackErr: ackErr}
		filtered := eventsender.FilteredFetcher(fetcher, filter, nil)
		_, _, err := filtered.Fetch(t.Context())
		require.ErrorIs(t, err, ackErr)
	})
}

func newEvent(name, namespace string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: name, Namespace: namespace},
	}
}
