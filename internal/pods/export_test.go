package pods

// IsDeleted reports whether the informer observed the deletion of the pod
// and still retains it.
func (i *Informer) IsDeleted(namespace, name string) bool {
	_, ok := i.deleted.get(podKey(namespace, name))
	return ok
}
