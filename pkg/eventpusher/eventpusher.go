package eventpusher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

// EventQueue is the interface the EventPusher uses to publish events to.
type EventQueue interface {
	PublishEvent(ctx context.Context, event *v1.Event) error
}

const (
	// eventChannelBufferSize is the size for events queued between the informer and the publisher goroutine.
	eventChannelBufferSize = 1024
	// defaultPushTimeout is the amount of time we allow pushes to the
	// queue to take
	defaultPushTimeout = 5 * time.Second

	// defaultResyncInterval is the interval for the forced resync of the
	// informers cache
	defaultResyncInterval = 4 * time.Hour
)

// EventPusher will watch Kubernetes events and pushes them to an event queue.
type EventPusher struct {
	eq EventQueue
	// factory is used setup event watches
	factory informers.SharedInformerFactory

	eventChannel chan *v1.Event
	ctx          context.Context
	cancel       context.CancelFunc
	logger       *slog.Logger

	startCtx       context.Context
	startCtxCancel context.CancelFunc

	pushTimeout time.Duration
	// used to track successful shutdown
	wg sync.WaitGroup
}

// EventPusherOptions can be used to alter behaviour for the event pusher
type EventPusherOptions struct {
	Logger         *slog.Logger
	PushTimeout    *time.Duration
	ResyncInterval *time.Duration
}

// NewEventPusher creates a new instance of EventPusher.
func NewEventPusher(ctx context.Context, eq EventQueue, k8sClient kubernetes.Interface, options EventPusherOptions) *EventPusher {
	ctx, cancel := context.WithCancel(ctx)

	resyncInterval := defaultResyncInterval
	if options.ResyncInterval != nil {
		resyncInterval = *options.ResyncInterval
	}

	factory := informers.NewSharedInformerFactory(k8sClient, resyncInterval)

	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	pushTimeout := options.PushTimeout
	if pushTimeout == nil {
		pushTimeout = ptr.To(defaultPushTimeout)
	}

	// we use this to indicate that we have successfully started
	startCtx, startCtxCancel := context.WithCancel(context.Background())

	return &EventPusher{
		eq:             eq,
		factory:        factory,
		eventChannel:   make(chan *v1.Event, eventChannelBufferSize),
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		pushTimeout:    *pushTimeout,
		startCtx:       startCtx,
		startCtxCancel: startCtxCancel,
	}
}

// registerEventHandler configures the K8s informer to send events to the internal channel.
func (ep *EventPusher) registerEventHandler() {
	informer := ep.factory.Core().V1().Events().Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if event, ok := obj.(*v1.Event); ok {
				ep.enqueue(event)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldEvent := oldObj.(*v1.Event)
			newEvent := newObj.(*v1.Event)

			// we only need to push the updated event if the
			// resource version has changed. Most probably this was
			// a `count` change.
			if oldEvent.ResourceVersion != newEvent.ResourceVersion {
				ep.enqueue(newEvent)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// no action needed for Kubernetes events.
		},
	})
	ep.logger.Debug("event handler registered")
}

// enqueue pushes the event onto the buffered channel.
func (ep *EventPusher) enqueue(event *v1.Event) {
	select {
	case ep.eventChannel <- event:
		// Successfully added to the buffer
	case <-ep.ctx.Done():
		// this wins over writing to a closed channel and indicates
		// that we should shutdown. So we just print a message.
		ep.logger.Debug("shutdown requested, not enqueuing event", slog.String("event-uid", string(event.UID)))
	}
}

// Run starts the informer and the event publishing loop. It should be started
// in a go-routine.
func (ep *EventPusher) Run() {
	ep.logger.Debug("starting Kubernetes informers and event publishing loop")

	// register the handler immediately to capture events as soon as the
	// informer starts.
	ep.registerEventHandler()

	stopCh := ep.ctx.Done()
	ep.factory.Start(stopCh)

	// wait for all caches to sync
	ep.logger.Debug("waiting for informer cache to be synced")
	if !cache.WaitForCacheSync(stopCh, ep.factory.Core().V1().Events().Informer().HasSynced) {
		ep.logger.Error("informer cache not synced...exiting")
		return
	}
	ep.logger.Debug("kubernetes informer caches synced")

	go ep.publishingLoop()
	// indicate that we started
	ep.startCtxCancel()

	// wait until we receive the signal to shutdown
	<-ep.ctx.Done()
	ep.logger.Debug("shutting down event pusher")
	// we close the event channel to indicate shutdown
	close(ep.eventChannel)
	// wait until everything is shut down
	ep.wg.Wait()
	ep.logger.Debug("event pusher shutdown complete")
}

// publishingLoop consumes events from the internal channel and publishes these events.
func (ep *EventPusher) publishingLoop() {
	ep.wg.Add(1)
	defer ep.wg.Done()
	ep.logger.Debug("publishing loop started")

	for {
		select {
		case event, ok := <-ep.eventChannel:
			if !ok {
				ep.logger.Debug("publishing loop shutdown")
				return
			}

			// Use a background context for publishing attempts if the main context is canceled
			publishCtx, publishCancel := context.WithTimeout(context.Background(), ep.pushTimeout)
			defer publishCancel()
			if err := ep.eq.PublishEvent(publishCtx, event); err != nil {
				ep.logger.Error("error on event publishing", slog.Any("error", err.Error()))
			}
		case <-ep.ctx.Done():
			// although the channel is our primary signal to stop, the loop continues processing remaining
			// items in the channel until the channel is closed by ep.Run().
			ep.logger.Debug("draining event buffer on shutdown")
		}
	}
}

// Stop initiates a graceful shutdown of the EventPusher and blocks until all
// internal goroutines have exited.
func (ep *EventPusher) Stop() {
	ep.cancel()
	// we wait for the publishing loop to exit
	ep.wg.Wait()
}

// WaitForStart blocks until the event pusher successfully started (or the
// maxTime time duration elapsed).
func (ep *EventPusher) WaitForStart(maxTime time.Duration) error {
	select {
	case <-ep.startCtx.Done():
		return nil
	case <-time.After(maxTime):
		return errors.New("timeout waiting for start")
	}
}
