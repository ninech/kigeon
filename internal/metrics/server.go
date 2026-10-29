package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves the metrics of a registry on /metrics, a liveness check on
// /healthz and a readiness check on /readyz.
type Server struct {
	server   *http.Server
	listener net.Listener
	ready    atomic.Bool
	logger   *slog.Logger
}

// NewServer listens on the given address. The server needs to be started
// with [Server.Serve].
func NewServer(addr string, gatherer prometheus.Gatherer, logger *slog.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s := &Server{listener: listener, logger: logger}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	s.server = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return s, nil
}

// Addr returns the address the server listens on.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Serve serves requests until the server is shut down.
func (s *Server) Serve() {
	if err := s.server.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Error("metrics server stopped", slog.String("error", err.Error()))
	}
}

// SetReady marks kigeon as ready to process events.
func (s *Server) SetReady() {
	s.ready.Store(true)
}

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
