package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kuberikcomv1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/slicer"
	"github.com/kuberik/release-controller/internal/source"
)

type q5Client struct {
	client.Client
	failStatus bool
}
type q5Status struct {
	client.SubResourceWriter
	c *q5Client
}

func (s q5Status) Update(ctx context.Context, obj client.Object, o ...client.SubResourceUpdateOption) error {
	if s.c.failStatus {
		s.c.failStatus = false
		return apierrors.NewServerTimeout(schema.GroupResource{Group: "kuberik.com", Resource: "releasesources"}, "update", 1)
	}
	return s.SubResourceWriter.Update(ctx, obj, o...)
}
func (c *q5Client) Status() client.SubResourceWriter { return q5Status{c.Client.Status(), c} }

var _ = Describe("rebuild then one failed status write", func() {
	const ns = "default"
	ctx := context.Background()
	n := 0
	run := func(label string, trigger func(name string)) {
		n++
		name := fmt.Sprintf("qa5-%d", n)
		fake := newFake()
		clock := baseTime.Add(time.Hour)
		qc := &q5Client{Client: k8sClient}
		r := &ReleaseSourceReconciler{
			Client: qc, Scheme: k8sClient.Scheme(),
			MaxReleasesPerSlice: 7, MaxSliceBytes: 256 * 1024, MaxResolvesPerSync: 10,
			Recorder: record.NewFakeRecorder(10000),
			Now:      func() time.Time { return clock },
			SourceFactory: func(context.Context, client.Reader, *kuberikcomv1alpha1.ReleaseSource) (source.Source, error) {
				return fake, nil
			},
		}
		rs := &kuberikcomv1alpha1.ReleaseSource{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: kuberikcomv1alpha1.ReleaseSourceSpec{
				OCI:       &kuberikcomv1alpha1.OCISource{Image: "example.com/app", Policy: &kuberikcomv1alpha1.OrderPolicy{Alphabetical: &kuberikcomv1alpha1.AlphabeticalPolicy{}}},
				Retention: kuberikcomv1alpha1.Retention{Days: ptr.To[int32](0), MinCount: ptr.To[int32](3)},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
		key := types.NamespacedName{Name: name, Namespace: ns}
		sync := func() error {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			clock = clock.Add(time.Second)
			return err
		}
		fake.addN(0, 40)
		for i := 0; i < 8; i++ {
			_ = sync()
		}
		trigger(name)
		qc.failStatus = true
		_, _ = fmt.Printf("QA5 %s rebuild sync err=%v\n", label, sync())
		for i := 0; i < 15; i++ {
			_ = sync()
		}
		var l kuberikcomv1alpha1.ReleaseSliceList
		Expect(k8sClient.List(ctx, &l, client.InNamespace(ns), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: name})).To(Succeed())
		got := &kuberikcomv1alpha1.ReleaseSource{}
		Expect(k8sClient.Get(ctx, key, got)).To(Succeed())
		_, _ = fmt.Printf("QA5 %s after 15 syncs: count=%d/40 backfillComplete=%v\n", label, len(slicer.Merge(l.Items)), got.Status.BackfillComplete)
		Expect(len(slicer.Merge(l.Items))).To(Equal(40), label)
		Expect(got.Status.BackfillComplete).To(BeTrue(), label)
	}
	It("identity change (spec image edit)", func() {
		run("image-edit", func(name string) {
			rs := &kuberikcomv1alpha1.ReleaseSource{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, rs)).To(Succeed())
			rs.Spec.OCI.Image = "example.com/app2"
			Expect(k8sClient.Update(ctx, rs)).To(Succeed())
		})
	})
	It("delete all slices (design 3a.8 rebuild)", func() {
		run("delete-all", func(name string) {
			Expect(k8sClient.DeleteAllOf(ctx, &kuberikcomv1alpha1.ReleaseSlice{}, client.InNamespace(ns), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: name})).To(Succeed())
		})
	})
})
