package pods_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/ninech/kigeon/internal/pods"
)

func TestInformer_GetPod(t *testing.T) {
	t.Parallel()

	t.Run("not started falls back to the API", func(t *testing.T) {
		t.Parallel()
		pod := newPod()
		informer := pods.NewInformer(fake.NewClientset(pod), pods.Options{})

		got, err := informer.GetPod(t.Context(), pod.Namespace, pod.Name)
		require.NoError(t, err)
		assert.Equal(t, pod.Name, got.Name)

		_, err = informer.GetPod(t.Context(), pod.Namespace, "missing")
		assert.True(t, k8serrors.IsNotFound(err))
	})

	t.Run("serves existing pods", func(t *testing.T) {
		t.Parallel()
		pod := newPod()
		informer := startInformer(t, fake.NewClientset(pod), time.Minute)

		got, err := informer.GetPod(t.Context(), pod.Namespace, pod.Name)
		require.NoError(t, err)
		assert.Equal(t, pod.UID, got.UID)
		assert.Empty(t, got.ManagedFields)
	})

	t.Run("serves deleted pods within the retention", func(t *testing.T) {
		t.Parallel()
		pod := newPod()
		client := fake.NewClientset(pod)
		informer := startInformer(t, client, time.Minute)

		require.NoError(t, client.CoreV1().Pods(pod.Namespace).Delete(t.Context(), pod.Name, metav1.DeleteOptions{}))
		waitForDeletion(t, informer, pod)

		got, err := informer.GetPod(t.Context(), pod.Namespace, pod.Name)
		require.NoError(t, err)
		assert.Equal(t, pod.UID, got.UID)
	})

	t.Run("remembers unknown pods", func(t *testing.T) {
		t.Parallel()
		client := fake.NewClientset()
		fakeClock := clocktesting.NewFakeClock(time.Now())
		informer := startInformerWithClock(t, client, time.Minute, fakeClock)

		for range 3 {
			_, err := informer.GetPod(t.Context(), "project", "gone")
			assert.True(t, k8serrors.IsNotFound(err))
		}
		assert.Equal(t, 1, podGets(client))

		// the pod is queried again once the TTL expired
		fakeClock.Step(pods.NotFoundTTL + time.Second)
		_, err := informer.GetPod(t.Context(), "project", "gone")
		assert.True(t, k8serrors.IsNotFound(err))
		assert.Equal(t, 2, podGets(client))
	})

	t.Run("forgets deleted pods after the retention", func(t *testing.T) {
		t.Parallel()
		pod := newPod()
		client := fake.NewClientset(pod)
		fakeClock := clocktesting.NewFakeClock(time.Now())
		informer := startInformerWithClock(t, client, time.Minute, fakeClock)

		require.NoError(t, client.CoreV1().Pods(pod.Namespace).Delete(t.Context(), pod.Name, metav1.DeleteOptions{}))
		waitForDeletion(t, informer, pod)

		fakeClock.Step(59 * time.Second)
		_, err := informer.GetPod(t.Context(), pod.Namespace, pod.Name)
		require.NoError(t, err)

		fakeClock.Step(2 * time.Second)
		_, err = informer.GetPod(t.Context(), pod.Namespace, pod.Name)
		assert.True(t, k8serrors.IsNotFound(err))
	})
}

func TestInformer_OnOOMKilled(t *testing.T) {
	t.Parallel()
	now := time.Now()
	pod := newPod(corev1.ContainerStatus{
		Name:                 "app",
		RestartCount:         1,
		LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now),
	})
	client := fake.NewClientset(pod)
	informer := startInformer(t, client, time.Minute)

	var mu sync.Mutex
	var received []*corev1.Event
	require.NoError(t, informer.OnOOMKilled(time.Hour, func(event *corev1.Event) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, event)
	}))
	receivedEvents := func() []*corev1.Event {
		mu.Lock()
		defer mu.Unlock()
		return append([]*corev1.Event(nil), received...)
	}

	// existing pods are reported once the handler gets registered
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, receivedEvents(), 1)
	}, 5*time.Second, 20*time.Millisecond)

	// an update without a new OOM kill does not report again
	updated := pod.DeepCopy()
	updated.Labels = map[string]string{"updated": "true"}
	_, err := client.CoreV1().Pods(pod.Namespace).Update(t.Context(), updated, metav1.UpdateOptions{})
	require.NoError(t, err)

	// another OOM kill gets reported
	updated = updated.DeepCopy()
	updated.Status.ContainerStatuses[0].RestartCount = 2
	updated.Status.ContainerStatuses[0].LastTerminationState = terminated(pods.OOMKilledReason, "containerd://2", now.Add(time.Minute))
	_, err = client.CoreV1().Pods(pod.Namespace).Update(t.Context(), updated, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, receivedEvents(), 2)
	}, 5*time.Second, 20*time.Millisecond)
	assert.Never(t, func() bool { return len(receivedEvents()) > 2 }, 200*time.Millisecond, 20*time.Millisecond)

	events := receivedEvents()
	assert.NotEqual(t, events[0].UID, events[1].UID)
}

func startInformer(t *testing.T, client *fake.Clientset, retention time.Duration) *pods.Informer {
	t.Helper()
	return startInformerWithClock(t, client, retention, clock.RealClock{})
}

func startInformerWithClock(t *testing.T, client *fake.Clientset, retention time.Duration, clock clock.Clock) *pods.Informer {
	t.Helper()
	informer := pods.NewInformerWithClock(client, pods.Options{DeletedPodRetention: retention}, clock)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, informer.Start(ctx))
	t.Cleanup(informer.Stop)
	return informer
}

// podGets returns the number of pod GET requests sent to the client.
func podGets(client *fake.Clientset) int {
	gets := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "pods" {
			gets++
		}
	}
	return gets
}

// waitForDeletion waits until the informer observed the deletion of the pod.
func waitForDeletion(t *testing.T, informer *pods.Informer, pod *corev1.Pod) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, informer.IsDeleted(pod.Namespace, pod.Name))
	}, 5*time.Second, 20*time.Millisecond)
}
