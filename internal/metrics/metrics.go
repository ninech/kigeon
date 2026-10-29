// Package metrics provides the Prometheus metrics of kigeon and the HTTP
// server exposing them together with health endpoints.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const namespace = "kigeon"

// Sources of watched events.
const (
	SourceKubernetes = "kubernetes"
	SourceSynthetic  = "synthetic"
)

// Results of publishing events to the queue.
const (
	PublishPublished = "published"
	PublishDuplicate = "duplicate"
	PublishError     = "error"
)

// Results of processing events in a sender.
const (
	ResultSent     = "sent"
	ResultFiltered = "filtered"
	ResultSkipped  = "skipped"
	ResultRejected = "rejected"
	ResultFailed   = "failed"
)

// Registry contains all kigeon metrics.
var Registry = prometheus.NewRegistry()

var (
	// EventsWatched counts the events received from Kubernetes and the
	// synthetic events created by kigeon.
	EventsWatched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_watched_total",
		Help:      "Events received from Kubernetes or created by kigeon.",
	}, []string{"source"})

	// EventsPublished counts the results of publishing events to the
	// queue.
	EventsPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_published_total",
		Help:      "Results of publishing events to the queue.",
	}, []string{"result"})

	// SenderEvents counts the results of processing events per sender.
	SenderEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "sender_events_total",
		Help:      "Results of processing events per sender.",
	}, []string{"sender", "result"})

	// SenderPushDuration observes the duration of pushes to the receiver
	// of a sender.
	SenderPushDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "sender_push_duration_seconds",
		Help:      "Duration of pushes to the receiver of a sender.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"sender"})

	// HookDuration observes the duration of hook executions, including
	// the pod enrichment.
	HookDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "hook_duration_seconds",
		Help:      "Duration of hook executions, including the pod enrichment.",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"sender"})

	// HookErrors counts failed hook executions.
	HookErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "hook_errors_total",
		Help:      "Failed hook executions.",
	}, []string{"sender"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		EventsWatched,
		EventsPublished,
		SenderEvents,
		SenderPushDuration,
		HookDuration,
		HookErrors,
	)
}
