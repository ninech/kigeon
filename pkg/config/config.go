// Package config provides configuration loading and validation for kigeon.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// Config represents the main configuration for Kigeon.
type Config struct {
	Global       GlobalConfig        `json:"global"`
	Queue        QueueConfig         `json:"queue"`
	Pusher       PusherConfig        `json:"pusher"`
	EventSenders []EventSenderConfig `json:"eventsenders"`
}

// GlobalConfig contains global application settings.
type GlobalConfig struct {
	// DataDir is the directory where Kigeon stores its data (NATS files).
	DataDir string `json:"dataDir"`
	// LogLevel is the logging level (debug, info, warn, error).
	LogLevel string `json:"logLevel"`
	// LogFormat is the log format (json, text).
	LogFormat string `json:"logFormat"`
}

// QueueConfig contains settings for the event queue.
type QueueConfig struct {
	// EventsMaxAge specifies how long events are stored in the queue.
	EventsMaxAge metav1.Duration `json:"eventsMaxAge"`
	// EventsMaxBytes is the maximum storage size for events (e.g., "500Mi", "1Gi").
	EventsMaxBytes resource.Quantity `json:"eventsMaxBytes"`
	// KubernetesEventsMaxLifetime is the TTL for processed event UIDs.
	KubernetesEventsMaxLifetime metav1.Duration `json:"kubernetesEventsMaxLifetime"`
}

// PusherConfig contains settings for the event pusher.
type PusherConfig struct {
	// PushTimeout is the maximum time to wait for pushing an event to the queue.
	PushTimeout metav1.Duration `json:"pushTimeout"`
}

// EventSenderConfig contains configuration for a single event sender.
type EventSenderConfig struct {
	// Name is the unique identifier for this sender.
	Name string `json:"name"`
	// Type is the sender type (e.g., "loki").
	Type string `json:"type"`
	// Filter contains optional namespace filtering configuration.
	Filter *FilterConfig `json:"filter,omitempty"`
	// Config contains sender-specific configuration as raw JSON.
	Config json.RawMessage `json:"config"`
}

// FilterConfig contains namespace filtering configuration.
type FilterConfig struct {
	// NamespaceSelector filters events based on namespace labels.
	NamespaceSelector *NamespaceSelectorConfig `json:"namespaceSelector,omitempty"`
	// IncludeNonNamespaced determines if non-namespaced objects should be included.
	IncludeNonNamespaced bool `json:"includeNonNamespaced"`
	// HardRefreshInterval is the interval for hard-refreshing the namespace list.
	HardRefreshInterval metav1.Duration `json:"hardRefreshInterval,omitempty"`
}

// NamespaceSelectorConfig contains label selector configuration.
type NamespaceSelectorConfig struct {
	// MatchLabels is a map of label key-value pairs that must match.
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// Load loads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return Parse(data)
}

// Parse parses configuration from YAML data.
func Parse(data []byte) (*Config, error) {
	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	applyDefaults(config)

	return config, nil
}

// applyDefaults sets default values for configuration options.
func applyDefaults(config *Config) {
	if config.Global.DataDir == "" {
		config.Global.DataDir = "/var/lib/kigeon/data"
	}
	if config.Global.LogLevel == "" {
		config.Global.LogLevel = "info"
	}
	if config.Global.LogFormat == "" {
		config.Global.LogFormat = "json"
	}

	if config.Queue.EventsMaxAge.Duration == 0 {
		config.Queue.EventsMaxAge = metav1.Duration{Duration: 24 * time.Hour}
	}
	if config.Queue.EventsMaxBytes.IsZero() {
		config.Queue.EventsMaxBytes = resource.MustParse("500Mi")
	}
	if config.Queue.KubernetesEventsMaxLifetime.Duration == 0 {
		config.Queue.KubernetesEventsMaxLifetime = metav1.Duration{Duration: 1 * time.Hour}
	}

	if config.Pusher.PushTimeout.Duration == 0 {
		config.Pusher.PushTimeout = metav1.Duration{Duration: 5 * time.Second}
	}
}
