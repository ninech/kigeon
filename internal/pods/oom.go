package pods

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// OOMKilledReason is the reason of synthetic events for containers
	// which were killed due to running out of memory.
	OOMKilledReason = "OOMKilled"
	// eventSourceComponent is the source component of synthetic events.
	eventSourceComponent = "kigeon"
)

// OOMKilledEvents returns a synthetic Warning event for every container of
// the pod which was killed due to running out of memory after since.
// Kubernetes itself only reports OOM kills as node events which can not be
// attributed to a pod.
//
// The event UID is derived from the terminated container instance, so the
// same OOM kill always results in the same event.
func OOMKilledEvents(pod *corev1.Pod, since time.Time) []*corev1.Event {
	var events []*corev1.Event
	seen := map[string]bool{}

	add := func(status corev1.ContainerStatus, fieldPath string, limits []corev1.Container) {
		for _, state := range []corev1.ContainerState{status.State, status.LastTerminationState} {
			terminated := state.Terminated
			if terminated == nil || terminated.Reason != OOMKilledReason || terminated.FinishedAt.Time.Before(since) {
				continue
			}
			id := terminated.ContainerID
			if id == "" {
				id = fmt.Sprintf("%s/%s/%s", pod.UID, status.Name, terminated.FinishedAt.UTC().Format(time.RFC3339))
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			events = append(events, oomKilledEvent(pod, status.Name, fieldPath, id, terminated, memoryLimit(limits, status.Name)))
		}
	}

	for _, status := range pod.Status.InitContainerStatuses {
		add(status, fmt.Sprintf("spec.initContainers{%s}", status.Name), pod.Spec.InitContainers)
	}
	for _, status := range pod.Status.ContainerStatuses {
		add(status, fmt.Sprintf("spec.containers{%s}", status.Name), pod.Spec.Containers)
	}
	return events
}

func oomKilledEvent(pod *corev1.Pod, container, fieldPath, id string, terminated *corev1.ContainerStateTerminated, limit string) *corev1.Event {
	hash := sha256.Sum256([]byte(id))
	suffix := hex.EncodeToString(hash[:8])

	message := fmt.Sprintf("Container %s was killed because it ran out of memory", container)
	if limit != "" {
		message += fmt.Sprintf(" (memory limit %s)", limit)
	}

	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.oomkilled.%s", pod.Name, suffix),
			Namespace: pod.Namespace,
			UID:       types.UID("kigeon-oomkilled-" + suffix),
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        pod.UID,
			FieldPath:  fieldPath,
		},
		Reason:              OOMKilledReason,
		Message:             message,
		Type:                corev1.EventTypeWarning,
		Count:               1,
		FirstTimestamp:      terminated.FinishedAt,
		LastTimestamp:       terminated.FinishedAt,
		Source:              corev1.EventSource{Component: eventSourceComponent, Host: pod.Spec.NodeName},
		ReportingController: eventSourceComponent,
	}
}

func memoryLimit(containers []corev1.Container, name string) string {
	for _, c := range containers {
		if c.Name != name {
			continue
		}
		if limit, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			return limit.String()
		}
	}
	return ""
}
