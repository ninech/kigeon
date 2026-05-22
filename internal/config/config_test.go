package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		yaml        string
		validate    func(t *testing.T, cfg *Config)
		expectError bool
	}{
		{
			name: "full config",
			yaml: `
global:
  dataDir: /data/kigeon
  logLevel: debug
  logFormat: text

queue:
  eventsMaxAge: 48h
  eventsMaxBytes: "1Gi"
  kubernetesEventsMaxLifetime: 2h

pusher:
  pushTimeout: 10s

eventsenders:
  - name: loki-prod
    type: loki
    filter:
      namespaceSelector:
        matchLabels:
          monitoring: "true"
      includeNonNamespaced: true
      hardRefreshInterval: 6h
    config:
      url: https://loki.example.com
      tenantID: production
`,
			validate: func(t *testing.T, cfg *Config) {
				t.Helper()
				assert.Equal(t, "/data/kigeon", cfg.Global.DataDir)
				assert.Equal(t, "debug", cfg.Global.LogLevel)
				assert.Equal(t, "text", cfg.Global.LogFormat)

				assert.Equal(t, 48*time.Hour, cfg.Queue.EventsMaxAge.Duration)
				assert.Equal(t, resource.MustParse("1Gi"), cfg.Queue.EventsMaxBytes)
				assert.Equal(t, 2*time.Hour, cfg.Queue.KubernetesEventsMaxLifetime.Duration)

				assert.Equal(t, 10*time.Second, cfg.Pusher.PushTimeout.Duration)

				require.Len(t, cfg.EventSenders, 1)
				sender := cfg.EventSenders[0]
				assert.Equal(t, "loki-prod", sender.Name)
				assert.Equal(t, "loki", sender.Type)
				require.NotNil(t, sender.Filter)
				assert.True(t, sender.Filter.IncludeNonNamespaced)
				assert.Equal(t, 6*time.Hour, sender.Filter.HardRefreshInterval.Duration)
				require.NotNil(t, sender.Filter.NamespaceSelector)
				assert.Equal(t, "true", sender.Filter.NamespaceSelector.MatchLabels["monitoring"])
				var senderConfig map[string]interface{}
				require.NoError(t, json.Unmarshal(sender.Config, &senderConfig))
				assert.Equal(t, "https://loki.example.com", senderConfig["url"])
				assert.Equal(t, "production", senderConfig["tenantID"])
			},
		},
		{
			name: "minimal config with defaults",
			yaml: `
eventsenders:
  - name: loki
    type: loki
    config:
      url: http://loki:3100
`,
			validate: func(t *testing.T, cfg *Config) {
				t.Helper()
				// Check defaults
				assert.Equal(t, "/var/lib/kigeon/data", cfg.Global.DataDir)
				assert.Equal(t, "info", cfg.Global.LogLevel)
				assert.Equal(t, "json", cfg.Global.LogFormat)

				assert.Equal(t, 24*time.Hour, cfg.Queue.EventsMaxAge.Duration)
				assert.Equal(t, resource.MustParse("500Mi"), cfg.Queue.EventsMaxBytes)
				assert.Equal(t, 1*time.Hour, cfg.Queue.KubernetesEventsMaxLifetime.Duration)

				assert.Equal(t, 5*time.Second, cfg.Pusher.PushTimeout.Duration)

				require.Len(t, cfg.EventSenders, 1)
				assert.Equal(t, "loki", cfg.EventSenders[0].Name)
			},
		},
		{
			name: "multiple senders",
			yaml: `
eventsenders:
  - name: loki-prod
    type: loki
    config:
      url: http://loki-prod:3100
  - name: loki-staging
    type: loki
    config:
      url: http://loki-staging:3100
`,
			validate: func(t *testing.T, cfg *Config) {
				t.Helper()
				require.Len(t, cfg.EventSenders, 2)
				assert.Equal(t, "loki-prod", cfg.EventSenders[0].Name)
				assert.Equal(t, "loki-staging", cfg.EventSenders[1].Name)
			},
		},
		{
			name: "noop sender",
			yaml: `
eventsenders:
  - name: loki-noop
    type: loki
    noop: true
    config:
      url: http://loki:3100
`,
			validate: func(t *testing.T, cfg *Config) {
				t.Helper()
				require.Len(t, cfg.EventSenders, 1)
				assert.True(t, cfg.EventSenders[0].Noop)
			},
		},
		{
			name:        "invalid yaml",
			yaml:        `invalid: [yaml`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Parse([]byte(tt.yaml))
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()
	t.Run("loads config from file", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		configFile := filepath.Join(dir, "config.yaml")

		yaml := `
global:
  dataDir: /test/data
eventsenders:
  - name: test
    type: loki
    config:
      url: http://loki:3100
`
		err := os.WriteFile(configFile, []byte(yaml), 0644)
		require.NoError(t, err)

		cfg, err := Load(configFile)
		require.NoError(t, err)
		assert.Equal(t, "/test/data", cfg.Global.DataDir)
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		t.Parallel()
		_, err := Load("/non/existent/path.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read config file")
	})
}
