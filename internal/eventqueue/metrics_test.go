package eventqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ninech/kigeon/internal/metrics"
)

// TestEventQueue_PublishMetrics is not parallel, as the counters are shared
// by all queues.
func TestEventQueue_PublishMetrics(t *testing.T) { //nolint:paralleltest
	eq := startTestQueue(t)
	defer eq.Stop()
	_, err := eq.Subscribe(t.Context(), "metrics")
	require.NoError(t, err)

	published := testutil.ToFloat64(metrics.EventsPublished.WithLabelValues(metrics.PublishPublished))
	duplicates := testutil.ToFloat64(metrics.EventsPublished.WithLabelValues(metrics.PublishDuplicate))

	event := newTestEvent("event", "uid-metrics", 1)
	require.NoError(t, eq.PublishEvent(t.Context(), event))
	require.NoError(t, eq.PublishEvent(t.Context(), event))

	assert.Equal(t, published+1, testutil.ToFloat64(metrics.EventsPublished.WithLabelValues(metrics.PublishPublished)))
	assert.Equal(t, duplicates+1, testutil.ToFloat64(metrics.EventsPublished.WithLabelValues(metrics.PublishDuplicate)))
}

func TestEventQueue_Collector(t *testing.T) {
	t.Parallel()
	eq := startTestQueue(t)
	defer eq.Stop()

	maxBytes := defaultTestOptions().EventsMaxBytes
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(eq.Collector()))

	fast, err := eq.Subscribe(t.Context(), "fast")
	require.NoError(t, err)
	_, err = eq.Subscribe(t.Context(), "slow")
	require.NoError(t, err)

	for i, uid := range []string{"uid-1", "uid-2", "uid-3"} {
		require.NoError(t, eq.PublishEvent(t.Context(), newTestEvent("event", uid, int32(i+1))))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, ack, err := fast.Fetch(ctx)
	require.NoError(t, err)
	require.NoError(t, ack.Ack())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		families, err := registry.Gather()
		require.NoError(c, err)
		got := map[string]float64{}
		for _, family := range families {
			for _, m := range family.GetMetric() {
				got[family.GetName()+labelSuffix(m)] = m.GetGauge().GetValue()
			}
		}
		assert.Equal(c, float64(3), got["kigeon_queue_messages"])
		assert.Positive(c, got["kigeon_queue_bytes"])
		assert.Equal(c, float64(maxBytes.Value()), got["kigeon_queue_max_bytes"])
		assert.Equal(c, float64(2), got["kigeon_sender_pending_events/fast"])
		assert.Equal(c, float64(3), got["kigeon_sender_pending_events/slow"])
	}, 5*time.Second, 50*time.Millisecond)
}

func labelSuffix(m *dto.Metric) string {
	suffix := ""
	for _, label := range m.GetLabel() {
		suffix += "/" + label.GetValue()
	}
	return suffix
}
