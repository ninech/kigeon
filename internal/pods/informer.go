// Package pods provides cached access to the pods of a cluster and detects
// containers which were killed due to running out of memory.
package pods

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	// defaultDeletedPodRetention is how long deleted pods stay available
	// by default.
	defaultDeletedPodRetention = 5 * time.Minute
	// notFoundTTL is how long pods which are neither known to the
	// informer nor to the Kubernetes API are remembered, to not query the
	// API for every event of such a pod.
	notFoundTTL = time.Minute
)

// Options configures the Informer.
type Options struct {
	Logger *slog.Logger
	// DeletedPodRetention defines how long deleted pods are still
	// returned by GetPod. Events are often processed after the pod they
	// refer to was deleted (e.g. for short-lived job pods).
	DeletedPodRetention time.Duration
}

// Informer watches all pods of the cluster. Until it is started, GetPod
// falls back to querying the Kubernetes API.
type Informer struct {
	client   kubernetes.Interface
	factory  informers.SharedInformerFactory
	informer cache.SharedIndexInformer
	lister   corelisters.PodLister
	deleted  *expiringCache[*corev1.Pod]
	notFound *expiringCache[struct{}]
	logger   *slog.Logger

	enabled  atomic.Bool
	started  atomic.Bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewInformer creates a new pod informer.
func NewInformer(client kubernetes.Interface, options Options) *Informer {
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	retention := options.DeletedPodRetention
	if retention == 0 {
		retention = defaultDeletedPodRetention
	}

	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithTransform(stripManagedFields))
	podInformer := factory.Core().V1().Pods()

	i := &Informer{
		client:   client,
		factory:  factory,
		informer: podInformer.Informer(),
		lister:   podInformer.Lister(),
		deleted:  newExpiringCache[*corev1.Pod](retention),
		notFound: newExpiringCache[struct{}](notFoundTTL),
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
	return i
}

// Enable marks the informer as required, see [Informer.Enabled].
func (i *Informer) Enable() {
	i.enabled.Store(true)
}

// Enabled reports whether a component requires the informer to be started.
func (i *Informer) Enabled() bool {
	return i.enabled.Load()
}

// Start starts watching pods and blocks until the cache is synced or the
// context is done.
func (i *Informer) Start(ctx context.Context) error {
	if _, err := i.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj any) {
			if pod := toPod(obj); pod != nil {
				i.deleted.add(podKey(pod.Namespace, pod.Name), pod)
			}
		},
	}); err != nil {
		return fmt.Errorf("failed to register pod delete handler: %w", err)
	}

	i.factory.Start(i.stopCh)
	for _, synced := range i.factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return errors.New("pod informer cache not synced")
		}
	}
	i.started.Store(true)
	return nil
}

// Stop stops watching pods and blocks until all handlers returned.
func (i *Informer) Stop() {
	i.stopOnce.Do(func() {
		close(i.stopCh)
		i.factory.Shutdown()
	})
}

// GetPod returns the pod with the given name. Deleted pods are returned
// within the configured retention. If the pod can not be found, an error
// satisfying [k8serrors.IsNotFound] is returned.
func (i *Informer) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if !i.started.Load() {
		return i.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	}

	pod, err := i.lister.Pods(namespace).Get(name)
	if err == nil {
		return pod, nil
	}
	if !k8serrors.IsNotFound(err) {
		return nil, err
	}
	key := podKey(namespace, name)
	if pod, ok := i.deleted.get(key); ok {
		return pod, nil
	}
	if _, ok := i.notFound.get(key); ok {
		return nil, err
	}

	// The pod might not be observed by the informer yet.
	pod, err = i.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		i.notFound.add(key, struct{}{})
	}
	return pod, err
}

func podKey(namespace, name string) string {
	return namespace + "/" + name
}

// OnOOMKilled calls fn for every container killed due to running out of
// memory within maxAge. Pods which exist when the handler is registered are
// checked as well. For updated pods, fn is only called for new OOM kills.
func (i *Informer) OnOOMKilled(maxAge time.Duration, fn func(*corev1.Event)) error {
	notify := func(oldPod, newPod *corev1.Pod) {
		since := time.Now().Add(-maxAge)
		known := map[string]bool{}
		if oldPod != nil {
			for _, event := range OOMKilledEvents(oldPod, since) {
				known[string(event.UID)] = true
			}
		}
		for _, event := range OOMKilledEvents(newPod, since) {
			if !known[string(event.UID)] {
				i.logger.Debug("container was OOM killed",
					slog.String("pod", newPod.Namespace+"/"+newPod.Name),
					slog.String("container", event.InvolvedObject.FieldPath),
				)
				fn(event)
			}
		}
	}

	_, err := i.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod := toPod(obj); pod != nil {
				notify(nil, pod)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPod, newPod := toPod(oldObj), toPod(newObj)
			if newPod != nil {
				notify(oldPod, newPod)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("failed to register OOM killed handler: %w", err)
	}
	return nil
}

func toPod(obj any) *corev1.Pod {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, _ := obj.(*corev1.Pod)
	return pod
}

// stripManagedFields removes managed fields from cached objects to reduce
// memory usage.
func stripManagedFields(obj any) (any, error) {
	if accessor, err := meta.Accessor(obj); err == nil {
		accessor.SetManagedFields(nil)
	}
	return obj, nil
}

// expiringCache keeps entries for a limited time.
type expiringCache[V any] struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]expiringEntry[V]
}

type expiringEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func newExpiringCache[V any](ttl time.Duration) *expiringCache[V] {
	return &expiringCache[V]{ttl: ttl, entries: map[string]expiringEntry[V]{}}
}

func (c *expiringCache[V]) add(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, k)
		}
	}
	c.entries[key] = expiringEntry[V]{value: value, expiresAt: now.Add(c.ttl)}
}

func (c *expiringCache[V]) get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || time.Now().After(entry.expiresAt) {
		var zero V
		return zero, false
	}
	return entry.value, true
}
