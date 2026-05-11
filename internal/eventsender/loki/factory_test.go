package loki

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name        string
		rawConfig   json.RawMessage
		setup       func(t *testing.T)
		validate    func(t *testing.T, cfg Config)
		expectError bool
	}{
		{
			name:      "valid full config",
			rawConfig: json.RawMessage(`{"url":"http://loki:3100","tenantID":"prod","streamLabels":{"app":"kigeon"},"batchSize":50}`),
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				assert.Equal(t, "http://loki:3100", cfg.URL)
				assert.Equal(t, "prod", cfg.TenantID)
				assert.Equal(t, map[string]string{"app": "kigeon"}, cfg.StreamLabels)
				assert.Equal(t, 50, cfg.BatchSize)
			},
		},
		{
			name:      "minimal config with only URL",
			rawConfig: json.RawMessage(`{"url":"http://loki:3100"}`),
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				assert.Equal(t, "http://loki:3100", cfg.URL)
				assert.Empty(t, cfg.TenantID)
				assert.Nil(t, cfg.BasicAuth)
				assert.Nil(t, cfg.StreamLabels)
			},
		},
		{
			name:      "basic auth with direct credentials",
			rawConfig: json.RawMessage(`{"url":"http://loki:3100","basicAuth":{"username":"user","password":"pass"}}`),
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				require.NotNil(t, cfg.BasicAuth)
				assert.Equal(t, "user", cfg.BasicAuth.Username)
				assert.Equal(t, "pass", cfg.BasicAuth.Password)
			},
		},
		{
			name:      "basic auth with env var references",
			rawConfig: json.RawMessage(`{"url":"http://loki:3100","basicAuth":{"usernameEnvVar":"LOKI_USER","passwordEnvVar":"LOKI_PASS"}}`),
			setup: func(t *testing.T) {
				t.Helper()
				t.Setenv("LOKI_USER", "env-user")
				t.Setenv("LOKI_PASS", "env-pass")
			},
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				require.NotNil(t, cfg.BasicAuth)
				assert.Equal(t, "env-user", cfg.BasicAuth.Username)
				assert.Equal(t, "env-pass", cfg.BasicAuth.Password)
			},
		},
		{
			name:      "direct credentials take precedence over env var references",
			rawConfig: json.RawMessage(`{"url":"http://loki:3100","basicAuth":{"username":"direct-user","usernameEnvVar":"LOKI_USER","password":"direct-pass","passwordEnvVar":"LOKI_PASS"}}`),
			setup: func(t *testing.T) {
				t.Helper()
				t.Setenv("LOKI_USER", "env-user")
				t.Setenv("LOKI_PASS", "env-pass")
			},
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				require.NotNil(t, cfg.BasicAuth)
				assert.Equal(t, "direct-user", cfg.BasicAuth.Username)
				assert.Equal(t, "direct-pass", cfg.BasicAuth.Password)
			},
		},
		{
			name:        "invalid JSON",
			rawConfig:   json.RawMessage(`{invalid`),
			expectError: true,
		},
		{
			name:        "wrong type for field",
			rawConfig:   json.RawMessage(`{"url":"http://loki:3100","batchSize":"not-a-number"}`),
			expectError: true,
		},
		{
			name:      "valid hook config",
			rawConfig: json.RawMessage(`{"url":"http://loki:3100","hook":{"script":"/etc/kigeon/hook.star","timeout":100000000,"onError":"skip"}}`),
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				require.NotNil(t, cfg.Hook)
				assert.Equal(t, "/etc/kigeon/hook.star", cfg.Hook.Script)
				assert.Equal(t, "skip", cfg.Hook.OnError)
			},
		},
		{
			name:        "hook missing script",
			rawConfig:   json.RawMessage(`{"url":"http://loki:3100","hook":{}}`),
			expectError: true,
		},
		{
			name:        "hook invalid onError value",
			rawConfig:   json.RawMessage(`{"url":"http://loki:3100","hook":{"script":"/etc/kigeon/hook.star","onError":"invalid"}}`),
			expectError: true,
		},
		{
			name:      "empty object config",
			rawConfig: json.RawMessage(`{}`),
			validate: func(t *testing.T, cfg Config) {
				t.Helper()
				assert.Empty(t, cfg.URL)
				assert.Empty(t, cfg.TenantID)
				assert.Nil(t, cfg.BasicAuth)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup(t)
			}

			cfg, err := parseConfig(tt.rawConfig)

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
