package metrics

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientmetrics "k8s.io/client-go/tools/metrics"
)

var (
	// KubernetesRequests counts the requests to the Kubernetes API by
	// status code.
	KubernetesRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "kubernetes_requests_total",
		Help:      "Requests to the Kubernetes API by status code.",
	}, []string{"code", "method"})

	// KubernetesRequestRetries counts retried requests to the Kubernetes
	// API by status code.
	KubernetesRequestRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "kubernetes_request_retries_total",
		Help:      "Retried requests to the Kubernetes API by status code.",
	}, []string{"code", "method"})

	// KubernetesRequestDuration observes the duration of non-watch
	// requests to the Kubernetes API.
	KubernetesRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "kubernetes_request_duration_seconds",
		Help:      "Duration of non-watch requests to the Kubernetes API.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"verb"})
)

// RegisterKubernetesMetrics records the requests of all Kubernetes clients.
// client-go only accepts the first registration in a process.
func RegisterKubernetesMetrics() {
	clientmetrics.Register(clientmetrics.RegisterOpts{
		RequestResult:  requestResult{},
		RequestRetry:   requestRetry{},
		RequestLatency: requestLatency{},
	})
}

type requestResult struct{}

func (requestResult) Increment(_ context.Context, code, method, _ string) {
	KubernetesRequests.WithLabelValues(code, method).Inc()
}

type requestRetry struct{}

func (requestRetry) IncrementRetry(_ context.Context, code, method, _ string) {
	KubernetesRequestRetries.WithLabelValues(code, method).Inc()
}

type requestLatency struct{}

func (requestLatency) Observe(_ context.Context, verb string, _ url.URL, latency time.Duration) {
	KubernetesRequestDuration.WithLabelValues(verb).Observe(latency.Seconds())
}
