// Package filter provides namespace filtering for Kubernetes events based on
// label selectors.
package filter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	defaultHardRefreshInterval = 4 * time.Hour
)

// DynamicNamespaceFilter watches namespaces on the cluster and allows
// filtering events by namespace label selectors.
type DynamicNamespaceFilter struct {
	client kubernetes.Interface
	config DynamicNamespaceFilterConfig

	// thread-safe map of allowed namespaces (key is namespace name, value
	// is empty struct)
	allowedNamespaces map[string]struct{}
	mu                sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	informer cache.SharedInformer
	logger   *slog.Logger
}

// DynamicNamespaceFilterConfig configures a DynamicNamespaceFilter.
type DynamicNamespaceFilterConfig struct {
	// Filter events based on the involved objects namespace labels.
	LabelSelector labels.Selector
	// Filter non-namespaced involved objects. If the involved object of an
	// event has no namespace set and this field is set to `true`, the
	// event will not be filtered. Otherwise it will be filtered.
	IncludeNonNamespaced bool
	// HardRefreshInterval defines the interval when to hard refresh all
	// namespaces. This is just an additional safeguard against missed
	// watches from the API server and should not be set too high.
	HardRefreshInterval *time.Duration
	// Logger can be used to log events of the dynamic namespace filter
	Logger *slog.Logger
}

// StaticNamespaceFilterConfig configures a filter based on a fixed list of namespaces.
type StaticNamespaceFilterConfig struct {
	// Filter events based on a static list of namespaces.
	Namespaces []string
	// Filter non-namespaced involved objects. If the involved object of an
	// event has no namespace set and this field is set to `true`, the
	// event will not be filtered. Otherwise it will be filtered.
	IncludeNonNamespaced bool
}

// NewDynamicNamespaceFilter creates a new namespace filter which allows to
// filter Kubernetes events. It dynamically watches the namespaces on the
// cluster and filters them by the given labels.
func NewDynamicNamespaceFilter(c kubernetes.Interface, cfg DynamicNamespaceFilterConfig) *DynamicNamespaceFilter {
	ctx, cancel := context.WithCancel(context.Background())

	hardRefreshInterval := defaultHardRefreshInterval
	if cfg.HardRefreshInterval != nil {
		hardRefreshInterval = *cfg.HardRefreshInterval
	}

	factory := informers.NewSharedInformerFactory(c, hardRefreshInterval)
	nsInformer := factory.Core().V1().Namespaces().Informer()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	dnf := &DynamicNamespaceFilter{
		client:            c,
		config:            cfg,
		allowedNamespaces: make(map[string]struct{}),
		ctx:               ctx,
		cancel:            cancel,
		informer:          nsInformer,
		logger:            logger,
	}

	// register event handlers
	if _, err := dnf.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ns, is := obj.(*corev1.Namespace)
			if !is {
				return
			}
			dnf.logger.Debug("found new namespace on API", slog.String("name", ns.Name))
			dnf.handleNamespaceChange(ns)
		},
		UpdateFunc: func(_, newObj interface{}) {
			ns, is := newObj.(*corev1.Namespace)
			if !is {
				return
			}
			dnf.logger.Debug("got update on namespace from API", slog.String("name", ns.Name))
			dnf.handleNamespaceChange(ns)
		},
		DeleteFunc: func(obj interface{}) {
			ns, is := obj.(*corev1.Namespace)
			if !is {
				return
			}
			dnf.logger.Debug("got namespace deletion event from API", slog.String("name", ns.Name))
			dnf.removeNamespace(ns.Name)
		},
	}); err != nil {
		dnf.logger.Error("failed to register namespace event handler", slog.Any("error", err))
	}

	return dnf
}

// Start starts the dynamic namespace filter. It returns an error if the
// namespace cache cannot be synced within the given context.
func (dnf *DynamicNamespaceFilter) Start(syncCtx context.Context) error {
	// increment the wait group immediately to prevent a race condition
	// with an immediate called `Stop()`.
	dnf.wg.Add(1)
	go func() {
		defer dnf.wg.Done()
		dnf.informer.Run(syncCtx.Done())
	}()

	dnf.logger.Debug("started watching for namespaces matching selector", slog.String("selector", dnf.config.LabelSelector.String()))

	if !cache.WaitForCacheSync(syncCtx.Done(), dnf.informer.HasSynced) {
		return errors.New("failed to sync namespace cache")
	}
	return nil
}

func (dnf *DynamicNamespaceFilter) handleNamespaceChange(ns *corev1.Namespace) {
	if dnf.config.LabelSelector.Matches(labels.Set(ns.Labels)) {
		dnf.addNamespace(ns.Name)
		return
	}
	// the namespace doesn't match our namespace selector so we eventually
	// need to remove it
	dnf.removeNamespace(ns.Name)
}
func (dnf *DynamicNamespaceFilter) addNamespace(name string) {
	dnf.mu.Lock()
	defer dnf.mu.Unlock()
	if _, exists := dnf.allowedNamespaces[name]; !exists {
		dnf.logger.Info("adding namespace to dynamic namespace filter", slog.String("name", name))
		dnf.allowedNamespaces[name] = struct{}{}
	}
}

func (dnf *DynamicNamespaceFilter) removeNamespace(name string) {
	dnf.mu.Lock()
	defer dnf.mu.Unlock()
	if _, exists := dnf.allowedNamespaces[name]; exists {
		dnf.logger.Info("removing namespace from dynamic namespace filter", slog.String("name", name))
		delete(dnf.allowedNamespaces, name)
	}
}

// IsAllowed checks if the given namespace is in the allowed list.
func (dnf *DynamicNamespaceFilter) IsAllowed(namespace string) bool {
	dnf.mu.RLock()
	defer dnf.mu.RUnlock()
	_, exists := dnf.allowedNamespaces[namespace]
	return exists
}

// IsAllowedForObject checks if the involved object should be included based
// on its namespace. For non-namespaced objects (empty namespace), it returns
// the IncludeNonNamespaced configuration value.
func (dnf *DynamicNamespaceFilter) IsAllowedForObject(obj corev1.ObjectReference) bool {
	if obj.Namespace == "" {
		return dnf.config.IncludeNonNamespaced
	}
	return dnf.IsAllowed(obj.Namespace)
}

// Stop stops the dynamic namespace filter and waits for all goroutines to exit.
func (dnf *DynamicNamespaceFilter) Stop() {
	dnf.cancel()
	dnf.wg.Wait()
}
