package eventqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// collectTimeout limits the time to query the queue state on a scrape.
const collectTimeout = 5 * time.Second

var (
	queueBytesDesc = prometheus.NewDesc("kigeon_queue_bytes",
		"Size of the events in the queue.", nil, nil)
	queueMessagesDesc = prometheus.NewDesc("kigeon_queue_messages",
		"Number of events in the queue.", nil, nil)
	queueMaxBytesDesc = prometheus.NewDesc("kigeon_queue_max_bytes",
		"Maximum size of the events in the queue. The oldest events are dropped once it is reached.", nil, nil)
	senderPendingDesc = prometheus.NewDesc("kigeon_sender_pending_events",
		"Events in the queue which a sender did not process yet.", []string{"sender"}, nil)
)

// Collector returns a Prometheus collector for the state of the queue and
// the backlog of each sender.
func (eq *EventQueue) Collector() prometheus.Collector {
	return &collector{eq: eq}
}

type collector struct {
	eq *EventQueue
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueBytesDesc
	ch <- queueMessagesDesc
	ch <- queueMaxBytesDesc
	ch <- senderPendingDesc
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()

	info, err := c.eq.jetStreamStream.Info(ctx)
	if err != nil {
		c.eq.logger.Warn("failed to get stream info for metrics", slog.String("error", err.Error()))
		return
	}
	ch <- prometheus.MustNewConstMetric(queueBytesDesc, prometheus.GaugeValue, float64(info.State.Bytes))
	ch <- prometheus.MustNewConstMetric(queueMessagesDesc, prometheus.GaugeValue, float64(info.State.Msgs))
	ch <- prometheus.MustNewConstMetric(queueMaxBytesDesc, prometheus.GaugeValue, float64(info.Config.MaxBytes))

	consumers := c.eq.jetStreamStream.ListConsumers(ctx)
	for consumer := range consumers.Info() {
		pending := consumer.NumPending + uint64(consumer.NumAckPending)
		ch <- prometheus.MustNewConstMetric(senderPendingDesc, prometheus.GaugeValue, float64(pending), consumer.Name)
	}
	if err := consumers.Err(); err != nil {
		c.eq.logger.Warn("failed to list consumers for metrics", slog.String("error", err.Error()))
	}
}
