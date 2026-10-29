package loki

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/ninech/kigeon/internal/eventqueue"
	"github.com/ninech/kigeon/internal/metrics"
)

// stubFetcher returns the given events once and blocks afterwards until the
// context is done.
type stubFetcher struct {
	mu     sync.Mutex
	events []*corev1.Event
	acks   []*mockAck
}

func (f *stubFetcher) Fetch(ctx context.Context) (*corev1.Event, eventqueue.EventAcknowledger, error) {
	f.mu.Lock()
	if len(f.events) == 0 {
		f.mu.Unlock()
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	event := f.events[0]
	f.events = f.events[1:]
	ack := &mockAck{}
	f.acks = append(f.acks, ack)
	f.mu.Unlock()
	return event, ack, nil
}

// lokiServer records the stream labels of all pushes and answers with status.
type lokiServer struct {
	mu     sync.Mutex
	pushes []map[string]string
}

func newLokiServer(t *testing.T, status int) (*httptest.Server, *lokiServer) {
	t.Helper()
	ls := &lokiServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload lokiPushPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil && len(payload.Streams) > 0 {
			ls.mu.Lock()
			ls.pushes = append(ls.pushes, payload.Streams[0].Stream)
			ls.mu.Unlock()
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server, ls
}

func (ls *lokiServer) labels() []map[string]string {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return append([]map[string]string(nil), ls.pushes...)
}

func TestSender_processEvent_streamLabels(t *testing.T) {
	t.Parallel()

	defaultLabels := map[string]string{
		"app":                  "kigeon",
		"namespace":            "default",
		"involved_object_kind": "Pod",
		"involved_object_name": "test-pod",
		"reason":               "Started",
		"type":                 "Normal",
		"source_component":     "kubelet",
		"source_host":          "node-1",
	}
	withLabel := func(key, value string) map[string]string {
		labels := map[string]string{}
		for k, v := range defaultLabels {
			labels[k] = v
		}
		labels[key] = value
		return labels
	}

	tests := map[string]struct {
		script     string
		onError    string
		wantLabels map[string]string
	}{
		"without hook, event labels override configured labels": {
			wantLabels: defaultLabels,
		},
		"hook receives the default labels": {
			script: `
def transform(config, event):
    if config["streamLabels"].get("reason") != "Started":
        fail("default labels missing")
    return config
`,
			onError:    "skip",
			wantLabels: defaultLabels,
		},
		"hook overrides a default label": {
			script: `
def transform(config, event):
    config["streamLabels"]["namespace"] = "tenant"
    return config
`,
			wantLabels: withLabel("namespace", "tenant"),
		},
		"hook replaces all labels": {
			script: `
def transform(config, event):
    config["streamLabels"] = {"app": "custom", "source": "event"}
    return config
`,
			wantLabels: map[string]string{"app": "custom", "source": "event"},
		},
		"failing hook with use-default sends the default labels": {
			script: `
def transform(config, event):
    fail("boom")
`,
			onError:    "use-default",
			wantLabels: defaultLabels,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, ls := newLokiServer(t, http.StatusNoContent)
			cfg := Config{
				URL:          server.URL,
				StreamLabels: map[string]string{"app": "kigeon", "namespace": "configured"},
			}
			if tc.script != "" {
				cfg.Hook = &ConfigHook{Script: writeStarScript(t, tc.script), OnError: tc.onError}
			}
			fetcher := &stubFetcher{events: []*corev1.Event{createTestEvent()}}
			sender, err := NewSender("test", cfg, fetcher, SenderOptions{Logger: testLogger()})
			require.NoError(t, err)

			require.NoError(t, sender.processEvent(t.Context()))

			require.Len(t, ls.labels(), 1)
			assert.Equal(t, tc.wantLabels, ls.labels()[0])
			require.Len(t, fetcher.acks, 1)
			assert.True(t, fetcher.acks[0].called)
			// the configured labels must not be modified
			assert.Equal(t, map[string]string{"app": "kigeon", "namespace": "configured"}, sender.config.StreamLabels)
		})
	}
}

func TestSender_processEvent_lokiResponses(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		status    int
		wantError bool
		wantAcked bool
	}{
		"accepted":            {status: http.StatusNoContent, wantAcked: true},
		"rejected as invalid": {status: http.StatusBadRequest, wantAcked: true},
		"rate limited":        {status: http.StatusTooManyRequests, wantError: true},
		"server error":        {status: http.StatusInternalServerError, wantError: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, _ := newLokiServer(t, tc.status)
			fetcher := &stubFetcher{events: []*corev1.Event{createTestEvent()}}
			sender, err := NewSender("test", Config{URL: server.URL}, fetcher, SenderOptions{Logger: testLogger()})
			require.NoError(t, err)

			err = sender.processEvent(t.Context())
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, fetcher.acks, 1)
			assert.Equal(t, tc.wantAcked, fetcher.acks[0].called)
		})
	}
}

func TestSender_processEvent_noopWithFailingHook(t *testing.T) {
	t.Parallel()
	server, ls := newLokiServer(t, http.StatusNoContent)
	cfg := Config{
		URL: server.URL,
		Hook: &ConfigHook{Script: writeStarScript(t, `
def transform(config, event):
    fail("boom")
`), OnError: "use-default"},
	}
	fetcher := &stubFetcher{events: []*corev1.Event{createTestEvent()}}
	sender, err := NewSender("test", cfg, fetcher, SenderOptions{Logger: testLogger(), Noop: true})
	require.NoError(t, err)

	require.NoError(t, sender.processEvent(t.Context()))
	assert.Empty(t, ls.labels(), "noop sender must not make HTTP requests")
	require.Len(t, fetcher.acks, 1)
	assert.True(t, fetcher.acks[0].called)
}

func TestSender_Run_returnsNilWhenStopped(t *testing.T) {
	t.Parallel()

	tests := map[string]func(sender *Sender, cancel context.CancelFunc){
		"stop":           func(sender *Sender, _ context.CancelFunc) { sender.Stop() },
		"context cancel": func(_ *Sender, cancel context.CancelFunc) { cancel() },
	}
	for name, stop := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sender, err := NewSender("test", Config{URL: "http://127.0.0.1:1"}, &stubFetcher{}, SenderOptions{Logger: testLogger()})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				result <- sender.Run(ctx)
			}()

			stop(sender, cancel)
			select {
			case err := <-result:
				assert.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return")
			}
		})
	}
}

func TestSender_processEvent_metrics(t *testing.T) {
	t.Parallel()

	const (
		skip    = "def transform(config, event):\n    return None\n"
		failing = "def transform(config, event):\n    fail(\"boom\")\n"
	)
	tests := map[string]struct {
		status     int
		script     string
		onError    string
		wantResult string
		wantError  bool
		wantCode   string
		hookError  bool
	}{
		"sent":                {status: http.StatusNoContent, wantResult: metrics.ResultSent, wantCode: "204"},
		"rejected":            {status: http.StatusBadRequest, wantResult: metrics.ResultRejected, wantCode: "400"},
		"failed":              {status: http.StatusInternalServerError, wantResult: metrics.ResultFailed, wantError: true, wantCode: "500"},
		"skipped by hook":     {status: http.StatusNoContent, script: skip, wantResult: metrics.ResultSkipped},
		"failed hook skipped": {status: http.StatusNoContent, script: failing, onError: "skip", wantResult: metrics.ResultSkipped, hookError: true},
		"failed hook retried": {status: http.StatusNoContent, script: failing, onError: "fail", wantResult: metrics.ResultFailed, wantError: true, hookError: true},
		"failed hook default": {status: http.StatusNoContent, script: failing, onError: "use-default", wantResult: metrics.ResultSent, wantCode: "204", hookError: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			senderName := "metrics-" + name
			server, _ := newLokiServer(t, tc.status)
			cfg := Config{URL: server.URL}
			if tc.script != "" {
				cfg.Hook = &ConfigHook{Script: writeStarScript(t, tc.script), OnError: tc.onError}
			}
			fetcher := &stubFetcher{events: []*corev1.Event{createTestEvent()}}
			sender, err := NewSender(senderName, cfg, fetcher, SenderOptions{Logger: testLogger()})
			require.NoError(t, err)

			err = sender.processEvent(t.Context())
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, float64(1), testutil.ToFloat64(metrics.SenderEvents.WithLabelValues(senderName, tc.wantResult)))
			wantHookErrors := float64(0)
			if tc.hookError {
				wantHookErrors = 1
			}
			assert.Equal(t, wantHookErrors, testutil.ToFloat64(metrics.HookErrors.WithLabelValues(senderName)))

			for _, code := range []string{"204", "400", "500"} {
				wantRequests := uint64(0)
				if code == tc.wantCode {
					wantRequests = 1
				}
				assert.Equal(t, float64(wantRequests), testutil.ToFloat64(metrics.SenderRequests.WithLabelValues(senderName, code)), "requests with code %s", code)
				assert.Equal(t, wantRequests, observations(t, metrics.SenderRequestDuration, senderName, code), "request durations with code %s", code)
			}
			wantHooks := uint64(0)
			if tc.script != "" {
				wantHooks = 1
			}
			assert.Equal(t, wantHooks, observations(t, metrics.HookDuration, senderName))
		})
	}
}

// observations returns the number of observations of a histogram.
func observations(t *testing.T, histogram *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, histogram.WithLabelValues(labels...).(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleCount()
}
