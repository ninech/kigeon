// Package loki implements an event sender that forwards Kubernetes events to
// a Grafana Loki instance.
package loki

import (
	"fmt"
	"os"
	"time"
)

// Config provides a config struct to configure the Loki Sender plugin.
type Config struct {
	// URL is the URL to a Loki instance (e.g., "http://loki:3100")
	URL string `json:"url"`
	// TenantID is the X-Scope-OrgID header value for multi-tenant Loki
	TenantID string `json:"tenantID,omitempty"`
	// BasicAuth configures basic authentication for Loki
	BasicAuth *BasicAuth `json:"basicAuth,omitempty"`
	// StreamLabels configure the stream labels to use when sending the
	// Kubernetes events to Loki. These labels are added to all log entries.
	StreamLabels map[string]string `json:"streamLabels,omitempty"`
	// BatchWait is the maximum time to wait before sending a batch of logs.
	// Defaults to 1 second if not set.
	BatchWait time.Duration `json:"batchWait,omitempty"`
	// BatchSize is the maximum number of log entries to batch before sending.
	// Defaults to 100 if not set.
	BatchSize int `json:"batchSize,omitempty"`
	// Hook configures an optional Starlark script that can dynamically modify
	// this config on a per-event basis before sending to Loki.
	Hook *ConfigHook `json:"hook,omitempty"`
}

// ConfigHook configures an embedded Starlark script executed per event to
// dynamically modify the Loki Config. The script must define a function:
//
//	def transform(config, event): ...
//
// where config and event are dicts and the function returns a modified config dict.
type ConfigHook struct {
	// Script is the path to a Starlark (.star) script file.
	Script string `json:"script"`
	// Timeout is the maximum time allowed for a single hook execution.
	// Defaults to 100ms if not set.
	Timeout time.Duration `json:"timeout,omitempty"`
	// OnError defines behavior when the hook fails.
	// "use-default" (default): use the original config and continue sending.
	// "skip": acknowledge the event without sending it.
	// "fail": return an error so NATS redelivers the event.
	OnError string `json:"onError,omitempty"`
	// EnrichPod, when true, fetches the full Pod definition from the Kubernetes
	// API and makes it available in the hook script as the top-level "pod" key
	// on the event dict. Has no effect if the involved object is not a Pod.
	//
	// Note: enrichment is best-effort. If the pod has already been
	// garbage-collected by the time the event is processed (e.g. for OOM
	// events on short-lived pods), the fetch will fail and "pod" will be
	// absent from the event dict. Hook scripts should always guard against
	// this with a fallback: event.get("pod") or {}.
	// Pod definitions are cached for 30 seconds. Since Kubernetes keeps
	// terminated pods in the API for ~2 minutes before garbage collection, a
	// warm cache from any prior event on the same pod is usually sufficient to
	// cover OOM events.
	EnrichPod bool `json:"enrichPod,omitempty"`
}

func (h *ConfigHook) validate() error {
	if h.Script == "" {
		return fmt.Errorf("hook script path is required")
	}
	switch h.OnError {
	case "", "use-default", "skip", "fail":
	default:
		return fmt.Errorf("invalid hook onError value %q, must be one of: use-default, skip, fail", h.OnError)
	}
	return nil
}

// BasicAuth holds basic authentication credentials.
// Username and Password can be set directly or via environment variable
// references (UsernameEnvVar, PasswordEnvVar). Direct values take precedence.
type BasicAuth struct {
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`
	UsernameEnvVar string `json:"usernameEnvVar,omitempty"`
	PasswordEnvVar string `json:"passwordEnvVar,omitempty"`
}

// resolveEnvVars populates Username and Password from environment variables
// when the direct values are empty and env var references are configured.
func (c *Config) resolveEnvVars() {
	if c.BasicAuth == nil {
		return
	}
	if c.BasicAuth.Username == "" && c.BasicAuth.UsernameEnvVar != "" {
		c.BasicAuth.Username = os.Getenv(c.BasicAuth.UsernameEnvVar)
	}
	if c.BasicAuth.Password == "" && c.BasicAuth.PasswordEnvVar != "" {
		c.BasicAuth.Password = os.Getenv(c.BasicAuth.PasswordEnvVar)
	}
}
