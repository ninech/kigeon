package eventqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// defaultBackOff defines the waiting time for unacknowledged messages before
// redelivering them to the consumer.
var defaultBackOff = []time.Duration{time.Second, 2 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute}

const (
	streamName   = "K8S_EVENTS_STREAM"
	subjectName  = "events.k8s"
	consumerName = "K8S_PROCESSOR"
	// Key-Value Bucket for storing processed UIDs (persistent deduplication).
	processedUIDsBucket = "PROCESSED_UIDS_KV"
	// maxAcknowledgeTime defines the max acknowledge time for messages
	// when a service is processing them
	maxAcknowledgeTime = 30 * time.Second
)

// EventQueue is used to push Kubernetes Events to and to retrieve them.
// Services can register in the queue to receive kubernetes events.
type EventQueue struct {
	server *server.Server
	// connection is the low-level connection to the NATS server
	connection *nats.Conn
	// jetStreamClient is the high-level connection to the NATS server
	jetStreamClient jetstream.JetStream
	// jetStreamStream allows to manage a stream and manage consumers for it.
	jetStreamStream jetstream.Stream
	// keyValue is a key value store used to deduplicate Kubernetes events
	// which already have been processed (and are thus not existing in the
	// queue anymore)
	keyValue jetstream.KeyValue
	// logger allows to log
	logger *slog.Logger
}

// EventQueueOptions allow to pass
type EventQueueOptions struct {
	Logger *slog.Logger
	Debug  bool
	// EventsMaxAge specifies how long we store (and retry sending for)
	// gathered Kubernetes Events.
	EventsMaxAge time.Duration
	// EventsMaxBytes configures the max size of Kubernetes events stored
	// in disk. Once this threshold was hit, the oldest messages will be
	// deleted (no matter if they have already been sent).
	EventsMaxBytes resource.Quantity
	// KubernetesEventsMaxLifetime passes the configured Kubernetes events
	// lifetime, which is 1 hour by default (if left empty).
	KubernetesEventsMaxLifetime *time.Duration
}

// StartEventQueue initializes and starts a new event queue.
func StartEventQueue(ctx context.Context, dataDir string, options EventQueueOptions) (*EventQueue, error) {
	// ensure the data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create event queue data directory: %w", err)
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	opts := &server.Options{
		Host: "127.0.0.1",
		// select a random port
		Port:      -1,
		JetStream: true,
		StoreDir:  dataDir,
		Debug:     options.Debug,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("error creating internal NATS server: %w", err)
	}

	ns.Start()
	logger.Debug("embedded NATS server started", slog.String("url", ns.ClientURL()))

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		return nil, fmt.Errorf("failed to connect NATS client: %w", err)
	}

	// create a new stream
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS JetStream client: %w", err)
	}

	eq := &EventQueue{
		server:          ns,
		connection:      nc,
		jetStreamClient: js,
		logger:          logger,
	}

	// set up the main stream for k8s events
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Subjects:    []string{subjectName},
		Description: "kubernetes events stream",
		Storage:     jetstream.FileStorage,
		MaxAge:      options.EventsMaxAge,
		MaxBytes:    options.EventsMaxBytes.Value(),
		// the duplication window is not needed as duplication is
		// checked via the key-value store, but we keep it as a safety
		// net
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to add stream: %w", err)
	}
	eq.jetStreamStream = stream
	logger.Debug("NATS JetStream stream configured and ready.", slog.String("stream-name", streamName))

	// set up a persistent key-value store used for deduplication of
	// already processed kubernetes events
	eventsLifeTime := options.KubernetesEventsMaxLifetime
	if eventsLifeTime == nil {
		eventsLifeTime = ptr.To(time.Hour)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  processedUIDsBucket,
		Storage: jetstream.FileStorage,
		// automatically clean up event UIDs to keep the KV size small
		TTL: *eventsLifeTime,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create KV deduplication store: %w", err)
	}
	eq.keyValue = kv
	logger.Debug("key-value store initialized for persistent deduplication", slog.String("max-age", eventsLifeTime.String()))

	return eq, nil
}

// EventFetcher allows to fetch new Kubernetes events.
type EventFetcher struct {
	consumer jetstream.Consumer
}

type EventAcknowledger interface {
	Ack() error
}

// Fetch fetches a single message from the event queue.
func (ef *EventFetcher) Fetch(ctx context.Context) (*corev1.Event, EventAcknowledger, error) {
	messageBatch, err := ef.consumer.Fetch(1, jetstream.FetchContext(ctx))
	if err != nil {
		return nil, nil, fmt.Errorf("can not fetch event: %w", err)
	}
	// wait for the message to arrive
	message := <-messageBatch.Messages()
	if messageBatch.Error() != nil {
		return nil, nil, fmt.Errorf("error during message fetching: %w", messageBatch.Error())
	}
	if message == nil {
		return nil, nil, fmt.Errorf("no message received: %w", ctx.Err())
	}
	event := &corev1.Event{}
	if err := json.Unmarshal(message.Data(), event); err != nil {
		return nil, nil, fmt.Errorf("can not JSON unmarshal message into an event: %w", err)
	}
	return event, message, nil
}

// Subscribe allows a service to subscribe to the gathered Kubernetes events.
// It returns an EventFetcher which can be used to fetch new events.
func (eq *EventQueue) Subscribe(ctx context.Context, service string) (*EventFetcher, error) {
	consumer, err := eq.jetStreamStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       service,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       maxAcknowledgeTime,
		BackOff:       defaultBackOff,
		// try to re-deliver a message for around 10 minutes (with the defaultBackOff).
		MaxDeliver: 11,
		// do not overwhelm the service with messages. Stop delivering
		// if there are max 2 unacknowledged event messages.
		MaxAckPending: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to add service: %w", err)
	}
	return &EventFetcher{consumer: consumer}, nil
}

// PublishEvent allows to publish a new Kubernetes event to all subscribed
// services. It checks for duplicated events (using the UID and `Count` of an event).
func (eq *EventQueue) PublishEvent(ctx context.Context, event *corev1.Event) error {
	// deep copy the event before storing it, as Informers may reuse/mutate objects.
	ev := event.DeepCopy()

	// Kubernetes events are largely immutable. Only the `count` and
	// `lastTimestamp` field get updates. As a change in the `count` field
	// should always happen when being updated, we integrate it into the
	// key used for deduplication checks.
	eventIdentifier := fmt.Sprintf("%s-%s", string(ev.GetUID()), strconv.Itoa(int(ev.Count)))
	logger := eq.logger.With(slog.String("event-id", eventIdentifier))

	// check the KV store (loaded from disk) for the event UID.
	// This solves a problem on restart: if the key exists, it means the event
	// was already processed successfully in a previous run.
	_, err := eq.keyValue.Get(ctx, eventIdentifier)
	if err == nil {
		logger.Debug("skipping already published event")
		return nil
	}
	if err != jetstream.ErrKeyNotFound {
		return fmt.Errorf("failed to check KV store for event UID %s: %w", eventIdentifier, err)
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("can not JSON marshal event %s: %w", eventIdentifier, err)
	}

	// publish the message using the K8s event UID as the Nats-Msg-Id for real-time deduplication.
	ack, err := eq.jetStreamClient.Publish(ctx, subjectName, data, jetstream.WithMsgID(eventIdentifier))
	if err != nil {
		if ack != nil && ack.Duplicate {
			logger.Info("duplicated event rejected")
			return nil
		}
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// Stop stops the event queue.
func (eq *EventQueue) Stop() {
	eq.logger.Info("stopping event queue")
	if eq.connection != nil {
		eq.connection.Close()
	}
	if eq.server != nil {
		eq.server.Shutdown()
	}
	eq.logger.Info("nats server stopped")
}
