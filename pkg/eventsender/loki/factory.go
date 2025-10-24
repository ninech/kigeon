package loki

import (
	"encoding/json"
	"fmt"

	"github.com/ninech/kigeon/pkg/eventsender"
	"github.com/ninech/kigeon/pkg/eventqueue"
)

const TypeName = "loki"

func init() {
	eventsender.Register(TypeName, Factory)
}

// Factory creates a new Loki sender from configuration.
func Factory(name string, rawConfig json.RawMessage, fetcher *eventqueue.EventFetcher, opts eventsender.FactoryOptions) (eventsender.EventSender, error) {
	config, err := parseConfig(rawConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to parse loki config: %w", err)
	}

	return NewSender(name, config, fetcher, SenderOptions{
		Logger:     opts.Logger,
		KubeClient: opts.KubeClient,
	})
}

// parseConfig unmarshals raw JSON config into a typed Loki Config struct,
// then resolves any environment variable references.
func parseConfig(rawConfig json.RawMessage) (Config, error) {
	var config Config
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return Config{}, fmt.Errorf("invalid loki configuration: %w", err)
	}
	config.resolveEnvVars()
	if config.Hook != nil {
		if err := config.Hook.validate(); err != nil {
			return Config{}, fmt.Errorf("invalid hook configuration: %w", err)
		}
	}
	return config, nil
}
