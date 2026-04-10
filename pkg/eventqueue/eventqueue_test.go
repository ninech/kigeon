package eventqueue_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ninech/kigeon/pkg/eventqueue"
)

func defaultTestOptions() eventqueue.Options {
	return eventqueue.Options{
		EventsMaxAge:   time.Hour,
		EventsMaxBytes: resource.MustParse("100Mi"),
	}
}

func startTestQueue(t *testing.T) *eventqueue.EventQueue {
	t.Helper()
	dataDir := t.TempDir()

	eq, err := eventqueue.StartEventQueue(t.Context(), dataDir, defaultTestOptions())
	require.NoError(t, err)
	require.NotNil(t, eq)

	return eq
}

func TestStartEventQueue(t *testing.T) {
	eq := startTestQueue(t)
	defer eq.Stop()
}

func TestEventQueue_PublishAndFetch(t *testing.T) {
	eq := startTestQueue(t)
	defer eq.Stop()

	// Subscribe before publishing
	fetcher, err := eq.Subscribe(t.Context(), "test-service")
	require.NoError(t, err)
	require.NotNil(t, fetcher)

	// Publish an event
	testEvent := newTestEvent("test-event-1", "test-uid-1", 1)
	err = eq.PublishEvent(t.Context(), testEvent)
	require.NoError(t, err)

	// Fetch the event
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	event, ack, err := fetcher.Fetch(ctx)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.NotNil(t, ack)

	assert.Equal(t, "test-event-1", event.Name)
	assert.Equal(t, "default", event.Namespace)
	assert.Equal(t, corev1.EventTypeNormal, event.Type)

	// Acknowledge the event
	err = ack.Ack()
	require.NoError(t, err)
}

func TestEventQueue_Deduplication(t *testing.T) {
	eq := startTestQueue(t)
	defer eq.Stop()

	// Subscribe first
	fetcher, err := eq.Subscribe(t.Context(), "test-service")
	require.NoError(t, err)

	// Publish the same event twice (same UID and count)
	testEvent := newTestEvent("test-event-1", "duplicate-uid", 1)
	err = eq.PublishEvent(t.Context(), testEvent)
	require.NoError(t, err)

	// Publish the exact same event again
	err = eq.PublishEvent(t.Context(), testEvent)
	require.NoError(t, err)

	// Fetch - should only get one event
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	event, ack, err := fetcher.Fetch(ctx)
	require.NoError(t, err)
	require.NotNil(t, event)
	err = ack.Ack()
	require.NoError(t, err)

	// Try to fetch another - should timeout since there's only one unique event
	ctx2, cancel2 := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel2()

	_, _, err = fetcher.Fetch(ctx2)
	require.Error(t, err) // Should error due to context timeout
}

func TestEventQueue_DifferentCountsAreNotDeduplicated(t *testing.T) {
	eq := startTestQueue(t)
	defer eq.Stop()

	// Subscribe first
	fetcher, err := eq.Subscribe(t.Context(), "test-service")
	require.NoError(t, err)

	// Publish event with count=1
	testEvent1 := newTestEvent("test-event-1", "same-uid", 1)
	err = eq.PublishEvent(t.Context(), testEvent1)
	require.NoError(t, err)

	// Publish same event with count=2 (simulating an update)
	testEvent2 := newTestEvent("test-event-1", "same-uid", 2)
	err = eq.PublishEvent(t.Context(), testEvent2)
	require.NoError(t, err)

	// Should be able to fetch both events
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	event1, ack1, err := fetcher.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(1), event1.Count)
	err = ack1.Ack()
	require.NoError(t, err)

	event2, ack2, err := fetcher.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(2), event2.Count)
	err = ack2.Ack()
	require.NoError(t, err)
}

func TestEventQueue_MultipleSubscribers(t *testing.T) {
	eq := startTestQueue(t)
	defer eq.Stop()

	// Create two subscribers
	fetcher1, err := eq.Subscribe(t.Context(), "service-1")
	require.NoError(t, err)

	fetcher2, err := eq.Subscribe(t.Context(), "service-2")
	require.NoError(t, err)

	// Publish an event
	testEvent := newTestEvent("shared-event", "shared-uid", 1)
	err = eq.PublishEvent(t.Context(), testEvent)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Both subscribers should receive the same event
	event1, ack1, err := fetcher1.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, "shared-event", event1.Name)
	err = ack1.Ack()
	require.NoError(t, err)

	event2, ack2, err := fetcher2.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, "shared-event", event2.Name)
	err = ack2.Ack()
	require.NoError(t, err)
}

func TestEventQueue_PersistenceAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()

	// Start the queue and publish an event
	eq1, err := eventqueue.StartEventQueue(t.Context(), dataDir, defaultTestOptions())
	require.NoError(t, err)

	testEvent := newTestEvent("persistent-event", "persistent-uid", 1)
	err = eq1.PublishEvent(t.Context(), testEvent)
	require.NoError(t, err)

	// Stop the queue
	eq1.Stop()

	// Restart with the same data directory
	eq2, err := eventqueue.StartEventQueue(t.Context(), dataDir, defaultTestOptions())
	require.NoError(t, err)
	defer eq2.Stop()

	// Try to publish the same event again - should be deduplicated via KV store
	err = eq2.PublishEvent(t.Context(), testEvent)
	require.NoError(t, err)

	// Subscribe and verify we can fetch the event (from stream persistence)
	fetcher, err := eq2.Subscribe(t.Context(), "test-service-new")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	event, ack, err := fetcher.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, "persistent-event", event.Name)
	err = ack.Ack()
	require.NoError(t, err)

	// There should only be one event (deduplication worked)
	ctx2, cancel2 := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel2()

	_, _, err = fetcher.Fetch(ctx2)
	require.Error(t, err) // Should timeout - no more events
}

func TestEventQueue_Stop(t *testing.T) {
	eq := startTestQueue(t)

	// Stop should not panic and should complete without error
	eq.Stop()
}

func TestStartEventQueue_CreatesDataDirectory(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := baseDir + "/nested/data/dir"

	// Verify the directory doesn't exist yet
	_, err := os.Stat(dataDir)
	require.True(t, os.IsNotExist(err))

	eq, err := eventqueue.StartEventQueue(t.Context(), dataDir, defaultTestOptions())
	require.NoError(t, err)
	defer eq.Stop()

	// Verify the directory was created
	info, err := os.Stat(dataDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func newTestEvent(name string, uid string, count int32) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "default",
			UID:             types.UID(uid),
			ResourceVersion: "1",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "test-pod",
			Namespace: "default",
		},
		Reason:         "TestReason",
		Message:        "Test event message",
		Type:           corev1.EventTypeNormal,
		Count:          count,
		FirstTimestamp: metav1.Now(),
		LastTimestamp:  metav1.Now(),
		Source: corev1.EventSource{
			Component: "test-component",
			Host:      "test-host",
		},
	}
}
