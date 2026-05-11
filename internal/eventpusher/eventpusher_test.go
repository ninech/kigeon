package eventpusher_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/ninech/kigeon/internal/eventpusher"
)

const (
	defaultNamespace = "default"
)

// fakeEventQueue implements the eventpusher.EventQueue interface for testing.
type fakeEventQueue struct {
	publishedEvents []*corev1.Event
	mu              sync.Mutex

	// used to verify if we really got all pushed events
	publishWait sync.WaitGroup
}

// PublishEvent tracks the event and signals the waiting test routine.
func (f *fakeEventQueue) PublishEvent(_ context.Context, event *corev1.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// deep copy the event before storing it, as Informers may reuse/mutate objects.
	copiedEvent := event.DeepCopy()
	f.publishedEvents = append(f.publishedEvents, copiedEvent)
	// decrement the amount of events we expect to receive
	f.publishWait.Done()
	return nil
}

// getEvents allows to retrieve the received queue events.
func (f *fakeEventQueue) getEvents() []*corev1.Event {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]*corev1.Event, len(f.publishedEvents))
	for i := range f.publishedEvents {
		result[i] = f.publishedEvents[i].DeepCopy()
	}
	return result
}

func TestEventPusher(t *testing.T) {
	t.Parallel()
	const (
		firstEvent, secondEvent = "pod-1234", "pod-5678"
	)
	for name, testCase := range map[string]struct {
		preHook        func(ctx context.Context, c *fake.Clientset) error
		postHook       func(ctx context.Context, c *fake.Clientset) error
		verify         func(events []*corev1.Event) error
		expectedEvents int
	}{
		"events created before the pusher starts will be sent": {
			preHook: func(ctx context.Context, c *fake.Clientset) error {
				_, err := c.CoreV1().Events("").CreateWithEventNamespaceWithContext(ctx, newEvent(firstEvent))
				return err
			},
			expectedEvents: 1,
			verify: func(events []*corev1.Event) error {
				if len(events) != 1 {
					return fmt.Errorf("expected 1 event to be received, but found %d", len(events))
				}
				if events[0].GetName() != firstEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", firstEvent, string(events[0].GetName()))
				}
				return nil
			},
		},
		"events created while the pusher is running will be sent": {
			postHook: func(ctx context.Context, c *fake.Clientset) error {
				_, err := c.CoreV1().Events("").CreateWithEventNamespaceWithContext(ctx, newEvent(firstEvent))
				return err
			},
			expectedEvents: 1,
			verify: func(events []*corev1.Event) error {
				if len(events) != 1 {
					return fmt.Errorf("expected 1 event to be received, but found %d", len(events))
				}
				if events[0].GetName() != firstEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", firstEvent, string(events[0].GetName()))
				}
				return nil
			},
		},
		"combined creation test": {
			preHook: func(ctx context.Context, c *fake.Clientset) error {
				_, err := c.CoreV1().Events("").CreateWithEventNamespaceWithContext(ctx, newEvent(firstEvent))
				return err
			},
			postHook: func(ctx context.Context, c *fake.Clientset) error {
				_, err := c.CoreV1().Events("").CreateWithEventNamespaceWithContext(ctx, newEvent(secondEvent))
				return err
			},
			expectedEvents: 2,
			verify: func(events []*corev1.Event) error {
				if len(events) != 2 {
					return fmt.Errorf("expected 2 events to be received, but found %d", len(events))
				}
				if events[0].GetName() != firstEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", firstEvent, string(events[0].GetName()))
				}
				if events[1].GetName() != secondEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", secondEvent, string(events[1].GetName()))
				}
				return nil
			},
		},
		"events with 'count' updates will be sent": {
			preHook: func(ctx context.Context, c *fake.Clientset) error {
				_, err := c.CoreV1().Events("").CreateWithEventNamespaceWithContext(ctx, newEvent(firstEvent))
				return err
			},
			postHook: func(ctx context.Context, c *fake.Clientset) error {
				event, err := c.CoreV1().Events(defaultNamespace).Get(ctx, firstEvent, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("error when retrieving event before update: %w", err)
				}
				event.Count = 42
				// we have to increase the ResourceVersion
				// manually as the fake clientset doesn't seem
				// to do this
				event.ResourceVersion = "2"
				_, err = c.CoreV1().Events("").UpdateWithEventNamespaceWithContext(ctx, event)
				return err
			},
			expectedEvents: 2,
			verify: func(events []*corev1.Event) error {
				if len(events) != 2 {
					return fmt.Errorf("expected 2 events to be received, but found %d", len(events))
				}
				if events[0].GetName() != firstEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", firstEvent, string(events[0].GetName()))
				}
				if events[1].GetName() != firstEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", secondEvent, string(events[1].GetName()))
				}
				if events[1].Count != 42 {
					return fmt.Errorf("expected the event to have a count of %d, but got %d", 42, events[1].Count)
				}
				return nil
			},
		},
		"event deletions are ignored": {
			postHook: func(ctx context.Context, c *fake.Clientset) error {
				_, err := c.CoreV1().Events("").CreateWithEventNamespaceWithContext(ctx, newEvent(firstEvent))
				if err != nil {
					return err
				}
				return c.CoreV1().Events(defaultNamespace).Delete(ctx, firstEvent, metav1.DeleteOptions{})
			},
			expectedEvents: 1,
			verify: func(events []*corev1.Event) error {
				if len(events) != 1 {
					return fmt.Errorf("expected 1 event to be received, but found %d", len(events))
				}
				if events[0].GetName() != firstEvent {
					return fmt.Errorf("expected one event with name %q, but got %q", firstEvent, string(events[0].GetName()))
				}
				if events[0].GetDeletionTimestamp() != nil {
					return errors.New("did not expect received event to have deletition timestamp, but got one")
				}
				return nil
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			is := require.New(t)
			fakeClient := fake.NewSimpleClientset()

			mockQueue := &fakeEventQueue{}
			handlerOptions := &slog.HandlerOptions{
				Level:     slog.LevelDebug,
				AddSource: true,
			}
			logger := slog.New(slog.NewJSONHandler(os.Stdout, handlerOptions)).With(slog.String("test-name", name))
			if testCase.preHook != nil {
				is.NoError(testCase.preHook(t.Context(), fakeClient))
			}
			mockQueue.publishWait.Add(testCase.expectedEvents)
			pusher := eventpusher.NewEventPusher(t.Context(), mockQueue, fakeClient, eventpusher.Options{
				Logger: logger,
				// we don't use a resync period as we want to count the events we
				// pushed. So relying on 'watch' should be fine.
				ResyncInterval: ptr.To(time.Duration(0)),
			})
			// lets start the pusher and wait for it to be ready
			go pusher.Run()
			is.NoError(pusher.WaitForStart(5 * time.Second))

			if testCase.postHook != nil {
				is.NoError(testCase.postHook(t.Context(), fakeClient))
			}

			// now wait until we see all expected events in the mock queue
			waitCh := make(chan struct{})
			go func() {
				mockQueue.publishWait.Wait()
				close(waitCh)
			}()

			t.Log("waiting for queue to receive events")
			select {
			case <-waitCh:
				// we received all events which we expected
			case <-time.After(5 * time.Second):
				jsonMarshaled, _ := json.Marshal(mockQueue.getEvents())
				t.Logf("event queue content after timeout: %s", string(jsonMarshaled))
				t.Fatal("ran into timeout while waiting for expected events to be published")
			}

			// allow to verify the received events
			if testCase.verify != nil {
				is.NoError(testCase.verify(mockQueue.getEvents()))
			}
			t.Log("testing graceful shutdown...")
			pusher.Stop()
			t.Log("pusher successfully shut down.")
		})
	}
}

func newEvent(name string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "default",
			ResourceVersion: "1",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: "my-pod",
		},
		Reason:  "Pulled",
		Message: "Container image already present on machine",
		Type:    corev1.EventTypeNormal,
	}
}
