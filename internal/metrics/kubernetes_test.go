package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/ninech/kigeon/internal/metrics"
)

// TestKubernetesMetrics is not parallel, as the client-go metrics are
// global.
func TestKubernetesMetrics(t *testing.T) { //nolint:paralleltest
	metrics.RegisterKubernetesMetrics()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
	}))
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)

	requests := testutil.ToFloat64(metrics.KubernetesRequests.WithLabelValues("403", http.MethodGet))
	durations := sampleCount(t, metrics.KubernetesRequestDuration.WithLabelValues(http.MethodGet))

	_, err = client.CoreV1().Pods("default").List(t.Context(), metav1.ListOptions{})
	require.True(t, k8serrors.IsForbidden(err), "unexpected error: %v", err)

	assert.Equal(t, requests+1, testutil.ToFloat64(metrics.KubernetesRequests.WithLabelValues("403", http.MethodGet)))
	assert.Equal(t, durations+1, sampleCount(t, metrics.KubernetesRequestDuration.WithLabelValues(http.MethodGet)))
}

func sampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleCount()
}
