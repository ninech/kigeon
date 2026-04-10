package loki

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func writeStarScript(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "hook.star")
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNewHookExecutor(t *testing.T) {
	t.Run("valid script", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    return config
`)
		hook, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)
		assert.NotNil(t, hook)
	})

	t.Run("script missing transform function", func(t *testing.T) {
		path := writeStarScript(t, `x = 1`)
		_, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transform")
	})

	t.Run("transform is not a function", func(t *testing.T) {
		path := writeStarScript(t, `transform = 42`)
		_, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transform")
	})

	t.Run("invalid starlark syntax", func(t *testing.T) {
		path := writeStarScript(t, `def transform(config, event)`)
		_, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.Error(t, err)
	})

	t.Run("non-existent script", func(t *testing.T) {
		_, err := newHookExecutor(ConfigHook{Script: "/does/not/exist.star"}, testLogger(), nil)
		require.Error(t, err)
	})
}

func TestHookExecutor_execute(t *testing.T) {
	baseConfig := Config{
		URL:          "http://loki:3100",
		TenantID:     "default",
		StreamLabels: map[string]string{"app": "kigeon"},
	}
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-event",
			Namespace: "production",
		},
		Reason:  "Started",
		Message: "Container started",
		Type:    corev1.EventTypeNormal,
	}

	t.Run("pass-through hook", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)

		result, err := h.execute(context.Background(), baseConfig, event)
		require.NoError(t, err)
		assert.Equal(t, "http://loki:3100", result.URL)
		assert.Equal(t, "default", result.TenantID)
	})

	t.Run("hook modifies tenantID based on namespace", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    ns = event["metadata"]["namespace"]
    if ns == "production":
        config["tenantID"] = "prod-tenant"
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)

		result, err := h.execute(context.Background(), baseConfig, event)
		require.NoError(t, err)
		assert.Equal(t, "prod-tenant", result.TenantID)
		assert.Equal(t, "http://loki:3100", result.URL)
	})

	t.Run("hook modifies streamLabels", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    labels = config.get("streamLabels") or {}
    labels["namespace"] = event["metadata"]["namespace"]
    config["streamLabels"] = labels
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)

		result, err := h.execute(context.Background(), baseConfig, event)
		require.NoError(t, err)
		assert.Equal(t, "production", result.StreamLabels["namespace"])
		assert.Equal(t, "kigeon", result.StreamLabels["app"])
	})

	t.Run("hook cannot change Hook field", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    return config
`)
		hookCfg := ConfigHook{Script: path, OnError: "fail"}
		cfgWithHook := baseConfig
		cfgWithHook.Hook = &hookCfg

		h, err := newHookExecutor(hookCfg, testLogger(), nil)
		require.NoError(t, err)

		result, err := h.execute(context.Background(), cfgWithHook, event)
		require.NoError(t, err)
		require.NotNil(t, result.Hook)
		assert.Equal(t, path, result.Hook.Script)
	})

	t.Run("hook times out", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    # Infinite loop — Starlark will terminate via thread.Cancel
    for _ in range(1000000000):
        pass
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path, Timeout: 50 * time.Millisecond}, testLogger(), nil)
		require.NoError(t, err)

		_, err = h.execute(context.Background(), baseConfig, event)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "hook timed out")
	})

	t.Run("hook returns wrong type", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    return "not a dict"
`)
		h, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)

		_, err = h.execute(context.Background(), baseConfig, event)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must return a dict")
	})

	t.Run("hook can read event reason", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    if event["reason"] == "OOMKilled":
        config["streamLabels"] = {"severity": "critical"}
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)

		oomEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "oom", Namespace: "default"},
			Reason:     "OOMKilled",
		}
		result, err := h.execute(context.Background(), baseConfig, oomEvent)
		require.NoError(t, err)
		assert.Equal(t, "critical", result.StreamLabels["severity"])
	})

	t.Run("hook can print for debugging", func(t *testing.T) {
		path := writeStarScript(t, `
def transform(config, event):
    print("processing event:", event["metadata"]["name"])
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path}, testLogger(), nil)
		require.NoError(t, err)

		_, err = h.execute(context.Background(), baseConfig, event)
		require.NoError(t, err)
	})

	t.Run("hook can read pod labels when enrichPodLabels is enabled", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "production",
				Labels:    map[string]string{"team": "platform", "env": "prod"},
			},
		}
		kubeClient := fake.NewClientset(pod)

		path := writeStarScript(t, `
def transform(config, event):
    labels = (event.get("pod") or {}).get("metadata", {}).get("labels") or {}
    if labels.get("team") == "platform":
        config["tenantID"] = "platform"
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path, EnrichPod: true}, testLogger(), kubeClient)
		require.NoError(t, err)

		podEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "ev", Namespace: "production"},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Name:      "test-pod",
				Namespace: "production",
			},
		}
		result, err := h.execute(context.Background(), baseConfig, podEvent)
		require.NoError(t, err)
		assert.Equal(t, "platform", result.TenantID)
	})

	t.Run("hook does not set podLabels for non-pod events", func(t *testing.T) {
		kubeClient := fake.NewClientset()

		path := writeStarScript(t, `
def transform(config, event):
    if "pod" in event:
        config["tenantID"] = "unexpected"
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path, EnrichPod: true}, testLogger(), kubeClient)
		require.NoError(t, err)

		deployEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "ev", Namespace: "production"},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Deployment",
				Name:      "my-deploy",
				Namespace: "production",
			},
		}
		result, err := h.execute(context.Background(), baseConfig, deployEvent)
		require.NoError(t, err)
		assert.Equal(t, "default", result.TenantID)
	})

	t.Run("pod labels are served from cache on second call", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cached-pod",
				Namespace: "default",
				Labels:    map[string]string{"cached": "true"},
			},
		}
		kubeClient := fake.NewClientset(pod)

		path := writeStarScript(t, `
def transform(config, event):
    return config
`)
		h, err := newHookExecutor(ConfigHook{Script: path, EnrichPod: true}, testLogger(), kubeClient)
		require.NoError(t, err)

		podEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "ev", Namespace: "default"},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Name:      "cached-pod",
				Namespace: "default",
			},
		}

		// First call — populates cache.
		_, err = h.execute(context.Background(), baseConfig, podEvent)
		require.NoError(t, err)

		// Delete the pod from the fake client; second call must still succeed via cache.
		err = kubeClient.CoreV1().Pods("default").Delete(context.Background(), "cached-pod", metav1.DeleteOptions{})
		require.NoError(t, err)

		_, err = h.execute(context.Background(), baseConfig, podEvent)
		require.NoError(t, err)
	})
}
