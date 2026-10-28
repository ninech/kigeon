package pods_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ninech/kigeon/internal/pods"
)

func TestOOMKilledEvents(t *testing.T) {
	t.Parallel()
	now := time.Now()
	since := now.Add(-time.Hour)

	tests := map[string]struct {
		pod            *corev1.Pod
		wantContainers []string
		wantFieldPaths []string
	}{
		"no terminated containers": {
			pod: newPod(corev1.ContainerStatus{Name: "app"}),
		},
		"terminated for another reason": {
			pod: newPod(corev1.ContainerStatus{
				Name:                 "app",
				LastTerminationState: terminated("Error", "containerd://1", now),
			}),
		},
		"oom killed and restarted": {
			pod: newPod(corev1.ContainerStatus{
				Name:                 "app",
				LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now),
			}),
			wantContainers: []string{"app"},
			wantFieldPaths: []string{"spec.containers{app}"},
		},
		"oom killed without restart": {
			pod: newPod(corev1.ContainerStatus{
				Name:  "app",
				State: terminated(pods.OOMKilledReason, "containerd://1", now),
			}),
			wantContainers: []string{"app"},
			wantFieldPaths: []string{"spec.containers{app}"},
		},
		"same container instance in state and last state": {
			pod: newPod(corev1.ContainerStatus{
				Name:                 "app",
				State:                terminated(pods.OOMKilledReason, "containerd://1", now),
				LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now),
			}),
			wantContainers: []string{"app"},
			wantFieldPaths: []string{"spec.containers{app}"},
		},
		"oom killed again after restart": {
			pod: newPod(corev1.ContainerStatus{
				Name:                 "app",
				State:                terminated(pods.OOMKilledReason, "containerd://2", now),
				LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now.Add(-time.Minute)),
			}),
			wantContainers: []string{"app", "app"},
			wantFieldPaths: []string{"spec.containers{app}", "spec.containers{app}"},
		},
		"oom killed before since": {
			pod: newPod(corev1.ContainerStatus{
				Name:                 "app",
				LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", since.Add(-time.Second)),
			}),
		},
		"init container": {
			pod: func() *corev1.Pod {
				p := newPod()
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{{
					Name:                 "migrate",
					LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now),
				}}
				return p
			}(),
			wantContainers: []string{"migrate"},
			wantFieldPaths: []string{"spec.initContainers{migrate}"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			events := pods.OOMKilledEvents(tc.pod, since)
			require.Len(t, events, len(tc.wantContainers))

			for i, event := range events {
				assert.Equal(t, pods.OOMKilledReason, event.Reason)
				assert.Equal(t, corev1.EventTypeWarning, event.Type)
				assert.Equal(t, int32(1), event.Count)
				assert.Equal(t, tc.pod.Namespace, event.Namespace)
				assert.NotEmpty(t, event.UID)
				assert.Equal(t, corev1.ObjectReference{
					Kind:       "Pod",
					APIVersion: "v1",
					Namespace:  tc.pod.Namespace,
					Name:       tc.pod.Name,
					UID:        tc.pod.UID,
					FieldPath:  tc.wantFieldPaths[i],
				}, event.InvolvedObject)
				assert.Equal(t, tc.pod.Spec.NodeName, event.Source.Host)
				assert.Contains(t, event.Message, tc.wantContainers[i])
				assert.False(t, event.LastTimestamp.IsZero())
			}
		})
	}
}

func TestOOMKilledEvents_DeterministicUID(t *testing.T) {
	t.Parallel()
	now := time.Now()

	first := pods.OOMKilledEvents(newPod(corev1.ContainerStatus{
		Name:                 "app",
		LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now),
	}), now.Add(-time.Hour))
	// the same container instance moved from state to last state
	again := pods.OOMKilledEvents(newPod(corev1.ContainerStatus{
		Name:  "app",
		State: terminated(pods.OOMKilledReason, "containerd://1", now),
	}), now.Add(-time.Hour))
	other := pods.OOMKilledEvents(newPod(corev1.ContainerStatus{
		Name:                 "app",
		LastTerminationState: terminated(pods.OOMKilledReason, "containerd://2", now),
	}), now.Add(-time.Hour))

	require.Len(t, first, 1)
	require.Len(t, again, 1)
	require.Len(t, other, 1)
	assert.Equal(t, first[0].UID, again[0].UID)
	assert.NotEqual(t, first[0].UID, other[0].UID)
}

func TestOOMKilledEvents_MemoryLimitInMessage(t *testing.T) {
	t.Parallel()
	now := time.Now()
	pod := newPod(corev1.ContainerStatus{
		Name:                 "app",
		LastTerminationState: terminated(pods.OOMKilledReason, "containerd://1", now),
	})
	pod.Spec.Containers = []corev1.Container{{
		Name: "app",
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		},
	}}

	events := pods.OOMKilledEvents(pod, now.Add(-time.Hour))
	require.Len(t, events, 1)
	assert.Contains(t, events[0].Message, "512Mi")
}

func newPod(statuses ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-7d9f8b6c5-x2x9z",
			Namespace: "project",
			UID:       "3c1f8f4e-4b8e-4b8e-9c1f-8f4e4b8e4b8e",
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			ContainerStatuses: statuses,
		},
	}
}

func terminated(reason, containerID string, finishedAt time.Time) corev1.ContainerState {
	return corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			Reason:      reason,
			ExitCode:    137,
			ContainerID: containerID,
			FinishedAt:  metav1.NewTime(finishedAt),
		},
	}
}
