package loki

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ninech/kigeon/pkg/eventqueue"
)

func TestNewSender(t *testing.T) {
	tests := []struct {
		name        string
		senderName  string
		config      Config
		fetcher     *eventqueue.EventFetcher
		expectError string
	}{
		{
			name:        "missing sender name",
			senderName:  "",
			config:      Config{URL: "http://localhost:3100"},
			fetcher:     &eventqueue.EventFetcher{},
			expectError: "sender name is required",
		},
		{
			name:        "missing URL",
			senderName:  "test-sender",
			config:      Config{},
			fetcher:     &eventqueue.EventFetcher{},
			expectError: "loki URL is required",
		},
		{
			name:        "missing event fetcher",
			senderName:  "test-sender",
			config:      Config{URL: "http://localhost:3100"},
			fetcher:     nil,
			expectError: "eventFetcher is required",
		},
		{
			name:       "valid config",
			senderName: "test-sender",
			config:     Config{URL: "http://localhost:3100"},
			fetcher:    &eventqueue.EventFetcher{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := NewSender(tt.senderName, tt.config, tt.fetcher, SenderOptions{})
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, sender)
			assert.Equal(t, tt.senderName, sender.Name())
		})
	}
}

func TestSender_sendToLoki(t *testing.T) {
	event := createTestEvent()

	tests := []struct {
		name           string
		config         Config
		serverResponse int
		serverBody     string
		expectError    string
		validateReq    func(t *testing.T, req *http.Request, body []byte)
	}{
		{
			name:           "successful send",
			config:         Config{},
			serverResponse: http.StatusNoContent,
			validateReq: func(t *testing.T, req *http.Request, body []byte) {
				assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
				assert.Equal(t, "/loki/api/v1/push", req.URL.Path)

				var payload lokiPushPayload
				err := json.Unmarshal(body, &payload)
				require.NoError(t, err)
				require.Len(t, payload.Streams, 1)
				assert.Equal(t, "default", payload.Streams[0].Stream["namespace"])
				assert.Equal(t, "Pod", payload.Streams[0].Stream["involved_object_kind"])
			},
		},
		{
			name: "with tenant ID",
			config: Config{
				TenantID: "test-tenant",
			},
			serverResponse: http.StatusNoContent,
			validateReq: func(t *testing.T, req *http.Request, _ []byte) {
				assert.Equal(t, "test-tenant", req.Header.Get("X-Scope-OrgID"))
			},
		},
		{
			name: "with basic auth",
			config: Config{
				BasicAuth: &BasicAuth{
					Username: "user",
					Password: "pass",
				},
			},
			serverResponse: http.StatusNoContent,
			validateReq: func(t *testing.T, req *http.Request, _ []byte) {
				user, pass, ok := req.BasicAuth()
				assert.True(t, ok)
				assert.Equal(t, "user", user)
				assert.Equal(t, "pass", pass)
			},
		},
		{
			name: "with stream labels",
			config: Config{
				StreamLabels: map[string]string{
					"app":     "kigeon",
					"cluster": "test",
				},
			},
			serverResponse: http.StatusNoContent,
			validateReq: func(t *testing.T, _ *http.Request, body []byte) {
				var payload lokiPushPayload
				err := json.Unmarshal(body, &payload)
				require.NoError(t, err)
				assert.Equal(t, "kigeon", payload.Streams[0].Stream["app"])
				assert.Equal(t, "test", payload.Streams[0].Stream["cluster"])
			},
		},
		{
			name:           "server error",
			config:         Config{},
			serverResponse: http.StatusInternalServerError,
			serverBody:     "internal error",
			expectError:    "loki returned status 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedReq *http.Request
			var capturedBody []byte

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedReq = r
				var err error
				capturedBody, err = io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("failed to read request body: %v", err)
				}
				if tt.serverBody != "" {
					w.WriteHeader(tt.serverResponse)
					if _, err := w.Write([]byte(tt.serverBody)); err != nil {
						t.Errorf("failed to write response body: %v", err)
					}
					return
				}
				w.WriteHeader(tt.serverResponse)
			}))
			defer server.Close()

			tt.config.URL = server.URL

			// create sender directly without eventFetcher for unit testing sendToLoki
			sender := &Sender{
				httpClient: server.Client(),
			}

			err := sender.sendToLoki(context.Background(), event, tt.config)

			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
				return
			}

			require.NoError(t, err)
			if tt.validateReq != nil {
				tt.validateReq(t, capturedReq, capturedBody)
			}
		})
	}
}

func TestSender_buildLabels(t *testing.T) {
	sender := &Sender{}
	cfg := Config{
		StreamLabels: map[string]string{
			"app": "kigeon",
		},
	}

	event := createTestEvent()
	labels := sender.buildLabels(event, cfg)

	assert.Equal(t, "kigeon", labels["app"])
	assert.Equal(t, "default", labels["namespace"])
	assert.Equal(t, "Pod", labels["involved_object_kind"])
	assert.Equal(t, "test-pod", labels["involved_object_name"])
	assert.Equal(t, "Started", labels["reason"])
	assert.Equal(t, "Normal", labels["type"])
	assert.Equal(t, "kubelet", labels["source_component"])
	assert.Equal(t, "node-1", labels["source_host"])
}

func TestSender_buildLabels_overridesStreamLabels(t *testing.T) {
	sender := &Sender{}
	cfg := Config{
		StreamLabels: map[string]string{
			"namespace": "should-be-overridden",
			"custom":    "label",
		},
	}

	event := createTestEvent()
	labels := sender.buildLabels(event, cfg)

	// Event-specific labels should override stream labels
	assert.Equal(t, "default", labels["namespace"])
	// Custom labels should still be present
	assert.Equal(t, "label", labels["custom"])
}

func TestSender_getEventTimestamp(t *testing.T) {
	sender := &Sender{}
	now := time.Now()

	tests := []struct {
		name     string
		event    *corev1.Event
		expected time.Time
	}{
		{
			name: "prefers EventTime",
			event: &corev1.Event{
				EventTime:      metav1.NewMicroTime(now),
				LastTimestamp:  metav1.NewTime(now.Add(-time.Hour)),
				FirstTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			expected: now,
		},
		{
			name: "falls back to LastTimestamp",
			event: &corev1.Event{
				LastTimestamp:  metav1.NewTime(now),
				FirstTimestamp: metav1.NewTime(now.Add(-time.Hour)),
			},
			expected: now,
		},
		{
			name: "falls back to FirstTimestamp",
			event: &corev1.Event{
				FirstTimestamp: metav1.NewTime(now),
			},
			expected: now,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sender.getEventTimestamp(tt.event)
			assert.Equal(t, tt.expected.Unix(), result.Unix())
		})
	}
}

func TestSender_formatEventMessage(t *testing.T) {
	sender := &Sender{}
	event := createTestEvent()

	message := sender.formatEventMessage(event)

	var parsed map[string]interface{}
	err := json.Unmarshal([]byte(message), &parsed)
	require.NoError(t, err)

	assert.Equal(t, "test-event", parsed["name"])
	assert.Equal(t, "default", parsed["namespace"])
	assert.Equal(t, "Pod", parsed["involvedObjectKind"])
	assert.Equal(t, "test-pod", parsed["involvedObjectName"])
	assert.Equal(t, "Started", parsed["reason"])
	assert.Equal(t, "Container started", parsed["message"])
	assert.Equal(t, "Normal", parsed["type"])
	assert.Equal(t, float64(1), parsed["count"])
}

func TestSender_buildPushPayload(t *testing.T) {
	sender := &Sender{}
	cfg := Config{
		StreamLabels: map[string]string{
			"app": "kigeon",
		},
	}

	event := createTestEvent()
	payload := sender.buildPushPayload(event, cfg)

	require.Len(t, payload.Streams, 1)
	stream := payload.Streams[0]

	// Check labels
	assert.Equal(t, "kigeon", stream.Stream["app"])
	assert.Equal(t, "default", stream.Stream["namespace"])

	// Check values (timestamp and message)
	require.Len(t, stream.Values, 1)
	require.Len(t, stream.Values[0], 2)
	// First element is timestamp as string
	assert.NotEmpty(t, stream.Values[0][0])
	// Second element is the JSON message
	assert.Contains(t, stream.Values[0][1], "test-event")
}

func createTestEvent() *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-event",
			Namespace: "default",
			UID:       types.UID("test-uid"),
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "test-pod",
			Namespace: "default",
		},
		Reason:  "Started",
		Message: "Container started",
		Type:    corev1.EventTypeNormal,
		Count:   1,
		Source: corev1.EventSource{
			Component: "kubelet",
			Host:      "node-1",
		},
		FirstTimestamp: metav1.Now(),
		LastTimestamp:  metav1.Now(),
	}
}
