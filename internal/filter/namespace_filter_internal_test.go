package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func TestDynamicNamespaceFilter_handleNamespaceDelete(t *testing.T) {
	t.Parallel()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "project", Labels: map[string]string{"env": "production"}}}

	tests := map[string]any{
		"namespace":                      ns,
		"tombstone of a missed deletion": cache.DeletedFinalStateUnknown{Key: ns.Name, Obj: ns},
	}
	for name, obj := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dnf := NewDynamicNamespaceFilter(fake.NewClientset(), DynamicNamespaceFilterConfig{
				LabelSelector: labels.SelectorFromSet(labels.Set{"env": "production"}),
			})
			dnf.handleNamespaceChange(ns)
			assert.True(t, dnf.IsAllowed(ns.Name))

			dnf.handleNamespaceDelete(obj)
			assert.False(t, dnf.IsAllowed(ns.Name))
		})
	}
}
