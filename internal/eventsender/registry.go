package eventsender

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"k8s.io/client-go/kubernetes"

	"github.com/ninech/kigeon/internal/eventqueue"
	"github.com/ninech/kigeon/internal/filter"
)

// FactoryOptions provides common options for creating event senders.
type FactoryOptions struct {
	Logger     *slog.Logger
	Filter     *filter.DynamicNamespaceFilter
	KubeClient kubernetes.Interface
}

// SenderFactory is a function that creates an EventSender from configuration.
type SenderFactory func(name string, rawConfig json.RawMessage, fetcher *eventqueue.EventFetcher, opts FactoryOptions) (EventSender, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]SenderFactory)
)

// Register adds a sender factory to the registry.
// This is typically called from init() functions in sender packages.
func Register(typeName string, factory SenderFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if factory == nil {
		panic("eventsender: Register factory is nil")
	}
	if _, dup := registry[typeName]; dup {
		panic("eventsender: Register called twice for type " + typeName)
	}
	registry[typeName] = factory
}

// Create instantiates an EventSender by type name using the registered factory.
func Create(typeName, name string, rawConfig json.RawMessage, fetcher *eventqueue.EventFetcher, opts FactoryOptions) (EventSender, error) {
	registryMu.RLock()
	factory, ok := registry[typeName]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown sender type: %s", typeName)
	}

	return factory(name, rawConfig, fetcher, opts)
}

// RegisteredTypes returns a list of all registered sender type names.
func RegisteredTypes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	types := make([]string, 0, len(registry))
	for typeName := range registry {
		types = append(types, typeName)
	}
	return types
}
