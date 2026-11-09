package controller

import (
	"context"
	"fmt"
	"sort"
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

// faultClient injects one slice write failure and can serve a stale slice
// list (an informer cache that has not seen the latest create yet).
type faultClient struct {
	client.Client
	failUpdate int // fail the Nth slice Update from now (1-based); 0 never
	failCreate bool
	updates    int
	stale      *kuberikcomv1alpha1.ReleaseSliceList
}

func (c *faultClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*kuberikcomv1alpha1.ReleaseSlice); ok {
		c.updates++
		if c.failUpdate > 0 && c.updates == c.failUpdate {
			return apierrors.NewServerTimeout(schema.GroupResource{Group: "kuberik.com", Resource: "releaseslices"}, "update", 1)
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *faultClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*kuberikcomv1alpha1.ReleaseSlice); ok && c.failCreate {
		c.failCreate = false
		return apierrors.NewServerTimeout(schema.GroupResource{Group: "kuberik.com", Resource: "releaseslices"}, "create", 1)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *faultClient) List(ctx context.Context, l client.ObjectList, opts ...client.ListOption) error {
	if sl, ok := l.(*kuberikcomv1alpha1.ReleaseSliceList); ok && c.stale != nil {
		c.stale.DeepCopyInto(sl)
		return nil
	}
	return c.Client.List(ctx, l, opts...)
}

var _ = Describe("Backfill walk under faults (QA round 2)", func() {
	const ns = "default"
	ctx := context.Background()
	var (
		fake  *fakeSource
		r     *ReleaseSourceReconciler
		fc    *faultClient
		name  string
		n     int
		clock time.Time
	)
	key := func() types.NamespacedName { return types.NamespacedName{Name: name, Namespace: ns} }
	reconcileOnce := func() (reconcile.Result, error) {
		return r.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
	}
	syncOK := func() reconcile.Result {
		res, err := reconcileOnce()
		Expect(err).NotTo(HaveOccurred())
		return res
	}
	list := func() []kuberikcomv1alpha1.ReleaseSlice {
		var l kuberikcomv1alpha1.ReleaseSliceList
		Expect(k8sClient.List(ctx, &l, client.InNamespace(ns), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: name})).To(Succeed())
		sort.Slice(l.Items, func(i, j int) bool { return l.Items[i].Name < l.Items[j].Name })
		return l.Items
	}
	// audit returns missing tag indexes, duplicate sequences and order violations.
	audit := func(total int) (count int, missing []int, dup, disorder int) {
		cat := slicer.Merge(list())
		in := map[string]bool{}
		seqs := map[int64]int{}
		for i, c := range cat {
			in[c.Tag] = true
			seqs[c.Sequence]++
			if i > 0 && cat[i-1].Tag > c.Tag {
				disorder++
			}
		}
		for _, v := range seqs {
			if v > 1 {
				dup++
			}
		}
		for i := 0; i < total; i++ {
			if !in[fakeTag(i)] {
				missing = append(missing, i)
			}
		}
		return len(cat), missing, dup, disorder
	}
	complete := func() bool {
		rs := &kuberikcomv1alpha1.ReleaseSource{}
		Expect(k8sClient.Get(ctx, key(), rs)).To(Succeed())
		return rs.Status.BackfillComplete
	}

	BeforeEach(func() {
		n++
		name = fmt.Sprintf("walk-%d", n)
		fake = newFake()
		clock = baseTime.Add(time.Hour)
		fc = &faultClient{Client: k8sClient}
		r = &ReleaseSourceReconciler{
			Client: fc, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			MaxReleasesPerSlice: 10, MaxSliceBytes: 256 * 1024, MaxResolvesPerSync: 10,
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
	})

	It("P-A: a failed slice write mid-backfill loses nothing", func() {
		r.MaxReleasesPerSlice = 100 // every batch updates the open slice
		fake.addN(0, 40)
		syncOK() // 39..30
		fc.failUpdate, fc.updates = 1, 0
		_, err := reconcileOnce() // 29..20 resolved but the write fails
		Expect(err).To(HaveOccurred())
		for i := 0; i < 10; i++ {
			syncOK()
		}
		count, missing, dup, disorder := audit(40)
		GinkgoWriter.Printf("P-A count=%d missing=%v dup=%d disorder=%d\n", count, missing, dup, disorder)
		Expect(missing).To(BeEmpty())
		Expect(dup + disorder).To(BeZero())
		Expect(count).To(Equal(40))
		Expect(complete()).To(BeTrue())
	})

	It("P-B: a stale slice cache with a fresh status cannot duplicate sequences", func() {
		fake.addN(0, 40)
		syncOK() // slice0 = 39..30 sealed
		before := list()
		syncOK()                                                       // slice1 = 29..20 sealed (create only)
		fc.stale = &kuberikcomv1alpha1.ReleaseSliceList{Items: before} // the informer has not seen slice1
		_, _ = reconcileOnce()
		fc.stale = nil
		for i := 0; i < 10; i++ {
			syncOK()
		}
		count, missing, dup, disorder := audit(40)
		GinkgoWriter.Printf("P-B count=%d missing=%v dup=%d disorder=%d\n", count, missing, dup, disorder)
		Expect(dup).To(BeZero())
		Expect(disorder).To(BeZero())
		Expect(missing).To(BeEmpty())
		Expect(count).To(Equal(40))
	})

	It("M1: incremental admission after a stale slice cache numbers from the truth", func() {
		fake.addN(0, 12)
		for i := 0; i < 3; i++ {
			syncOK()
		}
		Expect(complete()).To(BeTrue())
		before := list()
		fake.addN(12, 22)
		syncOK() // fills the open slice, seals it, opens another
		fc.stale = &kuberikcomv1alpha1.ReleaseSliceList{Items: before}
		fake.addN(22, 25)
		_, _ = reconcileOnce() // the informer has not seen the previous batch
		fc.stale = nil
		for i := 0; i < 3; i++ {
			syncOK()
		}
		count, missing, dup, disorder := audit(25)
		GinkgoWriter.Printf("M1 count=%d missing=%v dup=%d disorder=%d\n", count, missing, dup, disorder)
		Expect(dup).To(BeZero())
		Expect(disorder).To(BeZero())
		Expect(missing).To(BeEmpty())
		stored := 0
		for _, sl := range list() {
			stored += len(sl.Releases)
		}
		Expect(stored).To(Equal(count), "no tag may be stored in two slices")
	})

	It("M1: slices are read through the uncached reader, not the informer cache", func() {
		fake.addN(0, 12)
		syncOK()
		Expect(list()).NotTo(BeEmpty())
		fc.stale = &kuberikcomv1alpha1.ReleaseSliceList{} // a cache that has seen nothing
		rs := &kuberikcomv1alpha1.ReleaseSource{}
		Expect(k8sClient.Get(ctx, key(), rs)).To(Succeed())
		got, err := r.listSlices(ctx, rs)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeEmpty())
	})

	It("P-C: one transient failure on the first backfill tag loses nothing and backs off", func() {
		fake.addN(0, 40)
		syncOK()                            // 39..30
		fake.unresolved[fakeTag(29)] = true // a 429 that outlives one retry
		res := syncOK()
		Expect(res.RequeueAfter).To(BeNumerically(">=", 9*time.Second), "backed off, not an immediate retry")
		clock = clock.Add(res.RequeueAfter)
		res2 := syncOK()
		Expect(res2.RequeueAfter).To(BeNumerically(">", res.RequeueAfter), "exponential backoff")
		GinkgoWriter.Printf("P-C requeues %s then %s\n", res.RequeueAfter, res2.RequeueAfter)
		fake.unresolved[fakeTag(29)] = false // the registry recovers
		for i := 0; i < 10; i++ {
			clock = clock.Add(res2.RequeueAfter)
			res2 = syncOK()
		}
		count, missing, dup, disorder := audit(40)
		GinkgoWriter.Printf("P-C count=%d missing=%v dup=%d disorder=%d\n", count, missing, dup, disorder)
		Expect(missing).To(BeEmpty())
		Expect(dup + disorder).To(BeZero())
		Expect(count).To(Equal(40))
	})

	It("P4': permanently unresolvable tags are backed off, capped, excluded, and the walk completes", func() {
		fake.addN(0, 20)
		for i := 0; i < 15; i++ {
			fake.unresolved[fakeTag(i)] = true
		}
		var requeues []time.Duration
		for i := 0; i < 600; i++ {
			res := syncOK()
			requeues = append(requeues, res.RequeueAfter)
			clock = clock.Add(res.RequeueAfter)
			if complete() {
				break
			}
		}
		count, missing, dup, disorder := audit(20)
		GinkgoWriter.Printf("P4' syncs=%d resolves=%d elapsed=%s count=%d missing=%v\n", len(requeues), fake.resolves, clock.Sub(baseTime.Add(time.Hour)), count, missing)
		// The 15 unresolvable tags are excluded (not stubbed: a stub would bypass
		// the created rule); the 5 resolvable ones are all there.
		Expect(missing).To(Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}))
		Expect(dup + disorder).To(BeZero())
		Expect(count).To(Equal(5))
		Expect(complete()).To(BeTrue())
		// Backoff: the first requeues after a failure are not 1s hot loops.
		hot := 0
		for _, d := range requeues {
			if d <= time.Second {
				hot++
			}
		}
		Expect(hot).To(BeNumerically("<", len(requeues)/4), "retries must be backed off")
	})
})
