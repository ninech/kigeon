package metrics_test

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ninech/kigeon/internal/metrics"
)

func TestServer(t *testing.T) {
	t.Parallel()
	server, err := metrics.NewServer("127.0.0.1:0", metrics.Registry, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	go server.Serve()
	t.Cleanup(func() { _ = server.Shutdown(t.Context()) })

	get := func(path string) (int, string) {
		resp, err := http.Get("http://" + server.Addr() + path)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(body)
	}

	status, _ := get("/healthz")
	assert.Equal(t, http.StatusOK, status)

	status, _ = get("/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, status, "not ready before SetReady")
	server.SetReady()
	status, _ = get("/readyz")
	assert.Equal(t, http.StatusOK, status)

	metrics.SenderEvents.WithLabelValues("server-test", metrics.ResultSent).Inc()
	status, body := get("/metrics")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, `kigeon_sender_events_total{result="sent",sender="server-test"} 1`)
	assert.Contains(t, body, "go_goroutines")
}
