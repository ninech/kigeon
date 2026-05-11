package config

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		expectedErrors []string
	}{
		{
			name: "valid config",
			config: Config{
				Global: GlobalConfig{
					DataDir:   "/data",
					LogLevel:  "info",
					LogFormat: "json",
				},
				EventSenders: []EventSenderConfig{
					{
						Name:   "test-sender",
						Type:   "loki",
						Config: json.RawMessage(`{"url":"http://loki:3100"}`),
					},
				},
			},
			expectedErrors: nil,
		},
		{
			name: "missing data dir",
			config: Config{
				Global: GlobalConfig{
					DataDir: "",
				},
				EventSenders: []EventSenderConfig{
					{Name: "test", Type: "loki"},
				},
			},
			expectedErrors: []string{"global.dataDir"},
		},
		{
			name: "invalid log level",
			config: Config{
				Global: GlobalConfig{
					DataDir:  "/data",
					LogLevel: "invalid",
				},
				EventSenders: []EventSenderConfig{
					{Name: "test", Type: "loki"},
				},
			},
			expectedErrors: []string{"global.logLevel"},
		},
		{
			name: "invalid log format",
			config: Config{
				Global: GlobalConfig{
					DataDir:   "/data",
					LogFormat: "invalid",
				},
				EventSenders: []EventSenderConfig{
					{Name: "test", Type: "loki"},
				},
			},
			expectedErrors: []string{"global.logFormat"},
		},
		{
			name: "no event senders",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				EventSenders: []EventSenderConfig{},
			},
			expectedErrors: []string{"eventsenders"},
		},
		{
			name: "missing sender name",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				EventSenders: []EventSenderConfig{
					{Name: "", Type: "loki"},
				},
			},
			expectedErrors: []string{"eventsenders[0].name"},
		},
		{
			name: "missing sender type",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				EventSenders: []EventSenderConfig{
					{Name: "test", Type: ""},
				},
			},
			expectedErrors: []string{"eventsenders[0].type"},
		},
		{
			name: "duplicate sender names",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				EventSenders: []EventSenderConfig{
					{Name: "duplicate", Type: "loki"},
					{Name: "duplicate", Type: "loki"},
				},
			},
			expectedErrors: []string{"eventsenders[1].name"},
		},
		{
			name: "negative events max age",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				Queue: QueueConfig{
					EventsMaxAge: metav1.Duration{Duration: -1 * time.Hour},
				},
				EventSenders: []EventSenderConfig{
					{Name: "test", Type: "loki"},
				},
			},
			expectedErrors: []string{"queue.eventsMaxAge"},
		},
		{
			name: "negative events max bytes",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				Queue: QueueConfig{
					EventsMaxBytes: resource.MustParse("-1"),
				},
				EventSenders: []EventSenderConfig{
					{Name: "test", Type: "loki"},
				},
			},
			expectedErrors: []string{"queue.eventsMaxBytes"},
		},
		{
			name: "negative filter hard refresh interval",
			config: Config{
				Global: GlobalConfig{
					DataDir: "/data",
				},
				EventSenders: []EventSenderConfig{
					{
						Name: "test",
						Type: "loki",
						Filter: &FilterConfig{
							HardRefreshInterval: metav1.Duration{Duration: -1 * time.Hour},
						},
					},
				},
			},
			expectedErrors: []string{"eventsenders[0].filter.hardRefreshInterval"},
		},
		{
			name: "multiple errors",
			config: Config{
				Global: GlobalConfig{
					DataDir:  "",
					LogLevel: "invalid",
				},
				EventSenders: []EventSenderConfig{},
			},
			expectedErrors: []string{"global.dataDir", "global.logLevel", "eventsenders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if len(tt.expectedErrors) == 0 {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			errMsg := err.Error()
			for _, expected := range tt.expectedErrors {
				assert.Contains(t, errMsg, expected, "expected error for field %s", expected)
			}
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	err := ValidationError{
		Field:   "test.field",
		Message: "is invalid",
	}
	assert.Equal(t, "test.field: is invalid", err.Error())
}

func TestValidationErrors_Error(t *testing.T) {
	t.Run("empty errors", func(t *testing.T) {
		var errors ValidationErrors
		assert.Equal(t, "", errors.Error())
	})

	t.Run("single error", func(t *testing.T) {
		errors := ValidationErrors{
			{Field: "field1", Message: "is required"},
		}
		assert.Equal(t, "configuration validation failed: field1: is required", errors.Error())
	})

	t.Run("multiple errors", func(t *testing.T) {
		errors := ValidationErrors{
			{Field: "field1", Message: "is required"},
			{Field: "field2", Message: "is invalid"},
		}
		assert.Equal(t, "configuration validation failed: field1: is required; field2: is invalid", errors.Error())
	})
}
