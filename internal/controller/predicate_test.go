package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kuberikcomv1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
)

// M1: our own status writes must not trigger a reconcile (a registry listing);
// spec changes and annotation changes (force-sync) must.
func TestSourcePredicateIgnoresStatusOnlyUpdates(t *testing.T) {
	p := sourceChangedPredicate()
	old := &kuberikcomv1alpha1.ReleaseSource{ObjectMeta: metav1.ObjectMeta{Name: "x", Generation: 1}}
	statusOnly := old.DeepCopy()
	statusOnly.Status.Count = 7
	statusOnly.ResourceVersion = "2"
	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: statusOnly}) {
		t.Fatal("a status-only update must not reconcile")
	}
	spec := old.DeepCopy()
	spec.Generation = 2
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: spec}) {
		t.Fatal("a spec change must reconcile")
	}
	ann := old.DeepCopy()
	ann.Annotations = map[string]string{"reconcile.kuberik.com/requestedAt": "now"}
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: ann}) {
		t.Fatal("an annotation change must reconcile")
	}
	if !p.Create(event.CreateEvent{Object: old}) || !p.Delete(event.DeleteEvent{Object: old}) {
		t.Fatal("create and delete must reconcile")
	}
}
