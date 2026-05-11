package filter_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ninech/kigeon/internal/filter"
)

// eventually polls a condition function until it returns true or the timeout expires.
func eventually(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestNewDynamicNamespaceFilter(t *testing.T) {
	client := fake.NewSimpleClientset()

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})

	assert.NotNil(t, dnf)
}

func TestDynamicNamespaceFilter_Start(t *testing.T) {
	client := fake.NewSimpleClientset()

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start should return no error when cache syncs successfully
	err = dnf.Start(ctx)
	require.NoError(t, err)
}

func TestDynamicNamespaceFilter_StartReturnsErrorOnTimeout(t *testing.T) {
	client := fake.NewSimpleClientset()

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	// Use an already canceled context to force cache sync failure
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = dnf.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to sync namespace cache")
}

func TestDynamicNamespaceFilter_AddsMatchingNamespaces(t *testing.T) {
	// Create a namespace that matches the selector
	matchingNS := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "production-ns",
			Labels: map[string]string{
				"env": "production",
			},
		},
	}

	client := fake.NewSimpleClientset(matchingNS)

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = dnf.Start(ctx)
	require.NoError(t, err)

	// Verify the namespace was added
	assert.True(t, eventually(t, 2*time.Second, func() bool {
		return dnf.IsAllowed("production-ns")
	}), "expected production-ns to be allowed")
}

func TestDynamicNamespaceFilter_IgnoresNonMatchingNamespaces(t *testing.T) {
	// Create a namespace that doesn't match the selector
	nonMatchingNS := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "staging-ns",
			Labels: map[string]string{
				"env": "staging",
			},
		},
	}

	client := fake.NewSimpleClientset(nonMatchingNS)

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = dnf.Start(ctx)
	require.NoError(t, err)

	// Wait a bit and verify the namespace was never added
	time.Sleep(100 * time.Millisecond)
	assert.False(t, dnf.IsAllowed("staging-ns"))
}

func TestDynamicNamespaceFilter_HandlesNamespaceUpdates(t *testing.T) {
	// Start with a non-matching namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-namespace",
			Labels: map[string]string{
				"env": "staging",
			},
		},
	}

	client := fake.NewSimpleClientset(ns)

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = dnf.Start(ctx)
	require.NoError(t, err)

	// Initially should not be allowed (wait a bit for initial sync)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, dnf.IsAllowed("my-namespace"))

	// Update the namespace to match the selector
	ns.Labels["env"] = "production"
	_, err = client.CoreV1().Namespaces().Update(t.Context(), ns, metav1.UpdateOptions{})
	require.NoError(t, err)

	// Now should eventually be allowed
	assert.True(t, eventually(t, 2*time.Second, func() bool {
		return dnf.IsAllowed("my-namespace")
	}), "expected my-namespace to be allowed after update")
}

func TestDynamicNamespaceFilter_HandlesNamespaceDeletion(t *testing.T) {
	// Create a matching namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "production-ns",
			Labels: map[string]string{
				"env": "production",
			},
		},
	}

	client := fake.NewSimpleClientset(ns)

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = dnf.Start(ctx)
	require.NoError(t, err)

	// Initially should be allowed
	assert.True(t, eventually(t, 2*time.Second, func() bool {
		return dnf.IsAllowed("production-ns")
	}), "expected production-ns to be allowed initially")

	// Delete the namespace
	err = client.CoreV1().Namespaces().Delete(t.Context(), "production-ns", metav1.DeleteOptions{})
	require.NoError(t, err)

	// Should eventually no longer be allowed
	assert.True(t, eventually(t, 2*time.Second, func() bool {
		return !dnf.IsAllowed("production-ns")
	}), "expected production-ns to not be allowed after deletion")
}

func TestDynamicNamespaceFilter_IsAllowedForObject_WithNamespace(t *testing.T) {
	// Create a matching namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "production-ns",
			Labels: map[string]string{
				"env": "production",
			},
		},
	}

	client := fake.NewSimpleClientset(ns)

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = dnf.Start(ctx)
	require.NoError(t, err)

	// Wait for namespace to be processed
	assert.True(t, eventually(t, 2*time.Second, func() bool {
		return dnf.IsAllowed("production-ns")
	}))

	// Object in allowed namespace should be allowed
	allowedObj := corev1.ObjectReference{
		Kind:      "Pod",
		Name:      "my-pod",
		Namespace: "production-ns",
	}
	assert.True(t, dnf.IsAllowedForObject(allowedObj))

	// Object in non-allowed namespace should not be allowed
	notAllowedObj := corev1.ObjectReference{
		Kind:      "Pod",
		Name:      "my-pod",
		Namespace: "staging-ns",
	}
	assert.False(t, dnf.IsAllowedForObject(notAllowedObj))
}

func TestDynamicNamespaceFilter_IsAllowedForObject_NonNamespaced(t *testing.T) {
	client := fake.NewSimpleClientset()

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	clusterScopedObj := corev1.ObjectReference{
		Kind: "Node",
		Name: "node-1",
		// Namespace is empty for cluster-scoped objects
	}

	// Test with IncludeNonNamespaced=true
	dnfInclude := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector:        selector,
		IncludeNonNamespaced: true,
	})
	defer dnfInclude.Stop()

	err = dnfInclude.Start(ctx)
	require.NoError(t, err)
	assert.True(t, dnfInclude.IsAllowedForObject(clusterScopedObj))

	// Test with IncludeNonNamespaced=false
	dnfExclude := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector:        selector,
		IncludeNonNamespaced: false,
	})
	defer dnfExclude.Stop()

	err = dnfExclude.Start(ctx)
	require.NoError(t, err)
	assert.False(t, dnfExclude.IsAllowedForObject(clusterScopedObj))
}

func TestDynamicNamespaceFilter_CustomHardRefreshInterval(t *testing.T) {
	client := fake.NewSimpleClientset()

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	customInterval := 1 * time.Hour
	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector:       selector,
		HardRefreshInterval: &customInterval,
	})

	// Just verify it doesn't panic with custom interval
	assert.NotNil(t, dnf)
}

func TestDynamicNamespaceFilter_MatchesAllSelector(t *testing.T) {
	// Create multiple namespaces
	ns1 := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "namespace-1",
		},
	}
	ns2 := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "namespace-2",
		},
	}

	client := fake.NewSimpleClientset(ns1, ns2)

	// Empty selector matches everything
	selector := labels.Everything()

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})
	defer dnf.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := dnf.Start(ctx)
	require.NoError(t, err)

	// Both namespaces should eventually be allowed
	assert.True(t, eventually(t, 2*time.Second, func() bool {
		return dnf.IsAllowed("namespace-1") && dnf.IsAllowed("namespace-2")
	}), "expected both namespaces to be allowed")
}

func TestDynamicNamespaceFilter_Stop(t *testing.T) {
	client := fake.NewSimpleClientset()

	selector, err := labels.Parse("env=production")
	require.NoError(t, err)

	dnf := filter.NewDynamicNamespaceFilter(client, filter.DynamicNamespaceFilterConfig{
		LabelSelector: selector,
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = dnf.Start(ctx)
	require.NoError(t, err)

	// Stop should not panic and should complete
	dnf.Stop()
}
