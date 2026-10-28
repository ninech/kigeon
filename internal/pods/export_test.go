package pods

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
)

const NotFoundTTL = notFoundTTL

// NewInformerWithClock creates an informer using the given clock for the
// expiry of deleted and unknown pods.
func NewInformerWithClock(client kubernetes.Interface, options Options, clock clock.Clock) *Informer {
	return newInformer(client, options, clock)
}

// IsDeleted reports whether the informer observed the deletion of the pod
// and still retains it.
func (i *Informer) IsDeleted(namespace, name string) bool {
	_, ok := i.deleted.Get(podKey(namespace, name))
	return ok
}
