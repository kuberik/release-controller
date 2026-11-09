/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kuberikcomv1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/source"
)

var baseTime = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

// fakeSource is a controllable source. The policy orders by tag (the tags are
// zero-padded, so lexical is numeric).
type fakeSource struct {
	mu          sync.Mutex
	tags        []string
	created     map[string]time.Time // zero or missing: no created annotation
	unresolved  map[string]bool
	err         error
	notModified bool
	resolves    int
	rejects     map[string]string // tag -> permanent rejection reason
	ident       string
	skippedLong int
}

func newFake() *fakeSource {
	return &fakeSource{created: map[string]time.Time{}, unresolved: map[string]bool{}, rejects: map[string]string{}}
}

func fakeTag(i int) string {
	h := fnv.New64a()
	fmt.Fprint(h, i)
	return fmt.Sprintf("main-%010d-%040x", i, h.Sum64())
}

// add registers release i, created i minutes after baseTime.
func (f *fakeSource) add(i int) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := fakeTag(i)
	f.tags = append(f.tags, t)
	f.created[t] = baseTime.Add(time.Duration(i) * time.Minute)
	return t
}

func (f *fakeSource) addN(from, to int) {
	for i := from; i < to; i++ {
		f.add(i)
	}
}

func (f *fakeSource) Discover(_ context.Context, _ string) (source.DiscoverResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return source.DiscoverResult{}, f.err
	}
	var refs []source.Ref
	for _, t := range f.tags {
		refs = append(refs, source.Ref{Tag: t})
	}
	return source.DiscoverResult{Refs: refs, NotModified: f.notModified}, nil
}

func (f *fakeSource) Resolve(_ context.Context, ref source.Ref) (source.Release, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolves++
	if f.unresolved[ref.Tag] || f.rejects[ref.Tag] != "" {
		return source.Release{Tag: ref.Tag, MetadataUnresolved: true}, nil
	}
	h := fnv.New64a()
	fmt.Fprint(h, ref.Tag)
	sum := h.Sum64()
	r := source.Release{
		Tag:      ref.Tag,
		Digest:   ptr.To(fmt.Sprintf("sha256:%016x%016x%016x%016x", sum, sum, sum, sum)),
		Version:  ptr.To("1.4.2"),
		Revision: ptr.To(fmt.Sprintf("%040x", sum)),
		Requires: map[string]string{"database": "main-0000000001-" + fmt.Sprintf("%040x", sum), "cache": "v3.2.0"},
	}
	if c := f.created[ref.Tag]; !c.IsZero() {
		r.Created = metav1.NewTime(c)
	}
	return r, nil
}

func (f *fakeSource) Less(a, b source.Release) bool { return a.Tag < b.Tag }
func (f *fakeSource) Identity() string              { return f.ident }
func (f *fakeSource) SkippedLongTags() int          { return f.skippedLong }
func (f *fakeSource) Rejection(tag string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.rejects[tag]
	return m, ok
}

var _ = Describe("ReleaseSource Controller (fake source)", func() {
	const ns = "default"
	var (
		ctx   = context.Background()
		fake  *fakeSource
		rec   *record.FakeRecorder
		clock time.Time
		r     *ReleaseSourceReconciler
		name  string
		seq   int
	)

	create := func(mut func(*kuberikcomv1alpha1.ReleaseSource)) {
		rs := &kuberikcomv1alpha1.ReleaseSource{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: kuberikcomv1alpha1.ReleaseSourceSpec{
				OCI:       &kuberikcomv1alpha1.OCISource{Image: "example.com/app", Policy: &kuberikcomv1alpha1.OrderPolicy{Alphabetical: &kuberikcomv1alpha1.AlphabeticalPolicy{}}},
				Interval:  &metav1.Duration{Duration: time.Minute},
				Retention: kuberikcomv1alpha1.Retention{Days: ptr.To[int32](7), MinCount: ptr.To[int32](3)},
			},
		}
		if mut != nil {
			mut(rs)
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
	}
	key := func() types.NamespacedName { return types.NamespacedName{Name: name, Namespace: ns} }
	sync := func() reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
		Expect(err).NotTo(HaveOccurred())
		return res
	}
	getRS := func() *kuberikcomv1alpha1.ReleaseSource {
		rs := &kuberikcomv1alpha1.ReleaseSource{}
		Expect(k8sClient.Get(ctx, key(), rs)).To(Succeed())
		return rs
	}
	slices := func() []kuberikcomv1alpha1.ReleaseSlice {
		var l kuberikcomv1alpha1.ReleaseSliceList
		Expect(k8sClient.List(ctx, &l, client.InNamespace(ns), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: name})).To(Succeed())
		sort.Slice(l.Items, func(i, j int) bool { return l.Items[i].Name < l.Items[j].Name })
		return l.Items
	}
	rvs := func() map[string]string {
		m := map[string]string{}
		for _, s := range slices() {
			m[s.Name] = s.ResourceVersion
		}
		return m
	}
	sliceName := func(i int) string { return fmt.Sprintf("%s-%05d", name, i) }

	BeforeEach(func() {
		seq++
		name = fmt.Sprintf("fake-%d", seq)
		fake = newFake()
		rec = record.NewFakeRecorder(100)
		clock = baseTime.Add(time.Hour)
		r = &ReleaseSourceReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			MaxReleasesPerSlice: 5, MaxSliceBytes: 256 * 1024, MaxResolvesPerSync: 1000,
			Recorder: rec,
			Now:      func() time.Time { return clock },
			SourceFactory: func(context.Context, client.Reader, *kuberikcomv1alpha1.ReleaseSource) (source.Source, error) {
				return fake, nil
			},
		}
	})

	AfterEach(func() {
		for _, s := range slices() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &s))).To(Succeed())
		}
		rs := &kuberikcomv1alpha1.ReleaseSource{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, rs))).To(Succeed())
	})

	It("(a) a new release rewrites only the open slice; (b) seals at N", func() {
		create(nil)
		fake.addN(0, 12)
		res := sync()
		Expect(res.RequeueAfter).To(BeNumerically(">=", 54*time.Second))
		Expect(res.RequeueAfter).To(BeNumerically("<=", 66*time.Second))
		ss := slices()
		Expect(ss).To(HaveLen(3))
		Expect([]bool{ss[0].Sealed, ss[1].Sealed, ss[2].Sealed}).To(Equal([]bool{true, true, false}))
		Expect(ss[0].Releases).To(HaveLen(5))
		Expect(ss[2].Releases).To(HaveLen(2))
		Expect(ss[0].OwnerReferences).To(HaveLen(1))
		Expect(*ss[0].OwnerReferences[0].Controller).To(BeTrue())
		Expect(ss[0].Labels).To(HaveKeyWithValue(kuberikcomv1alpha1.ReleaseSourceLabel, name))
		st := getRS().Status
		Expect(st.Count).To(Equal(int32(12)))
		Expect(st.SliceCount).To(Equal(int32(3)))
		Expect(st.OpenSlice).To(Equal(sliceName(2)))
		Expect(st.BackfillComplete).To(BeTrue())
		Expect(st.NextSliceIndex).To(Equal(int32(3)))
		Expect(st.Newest.Tag).To(Equal(fakeTag(11)))
		Expect(meta.IsStatusConditionTrue(st.Conditions, kuberikcomv1alpha1.ReadyCondition)).To(BeTrue())

		before := rvs()
		fake.add(12)
		sync()
		after := rvs()
		Expect(after[sliceName(0)]).To(Equal(before[sliceName(0)]))
		Expect(after[sliceName(1)]).To(Equal(before[sliceName(1)]))
		Expect(after[sliceName(2)]).NotTo(Equal(before[sliceName(2)]))
		Expect(slices()[2].Releases).To(HaveLen(3))

		// two more fill the open slice: sealed in the same write, no empty open slice.
		fake.add(13)
		fake.add(14)
		sync()
		ss = slices()
		Expect(ss).To(HaveLen(3))
		Expect(ss[2].Sealed).To(BeTrue())
		Expect(rvs()[sliceName(1)]).To(Equal(before[sliceName(1)]))
	})

	It("(b) seals on the byte valve", func() {
		r.MaxReleasesPerSlice = 100
		r.MaxSliceBytes = 4000
		create(nil)
		fake.addN(0, 30)
		sync()
		ss := slices()
		Expect(len(ss)).To(BeNumerically(">=", 3))
		Expect(ss[0].Sealed).To(BeTrue())
		Expect(len(ss[0].Releases)).To(BeNumerically("<", 100))
		b, _ := json.Marshal(ss[0])
		Expect(len(b)).To(BeNumerically(">=", 4000*9/10)) // stored object carries managedFields; valve counts the object the controller built
	})

	It("(c) retention drops whole oldest sealed slices (by min sequence) and respects minCount", func() {
		create(nil)
		fake.addN(0, 12) // backfill: 00000 = 11..7, 00001 = 6..2, 00002 (open) = 1,0
		sync()
		// The open slice holds the lowest sequences, so it blocks retention
		// until it seals: nothing is deleted although everything is expired.
		clock = baseTime.Add(30 * 24 * time.Hour)
		sync()
		Expect(slices()).To(HaveLen(3))
		fake.addN(12, 15) // fills and seals the open slice (5); it now holds the newest release
		sync()
		Expect(slices()).To(HaveLen(3)) // the slice holding the newest release is never deleted
		fake.add(15)                    // a new open slice
		sync()
		// min-sequence order: 00002, 00001, 00000. minCount 3, 16 releases:
		// 00002 goes (11 left), 00001 goes (6 left), 00000 would leave 1 < 3.
		ss := slices()
		Expect(ss).To(HaveLen(2))
		Expect(ss[0].Name).To(Equal(sliceName(0)))
		Expect(ss[1].Name).To(Equal(sliceName(3)))
		Expect(getRS().Status.Count).To(Equal(int32(6)))

		rs := getRS()
		rs.Spec.Retention.MinCount = ptr.To[int32](0)
		Expect(k8sClient.Update(ctx, rs)).To(Succeed())
		sync()
		ss = slices()
		Expect(ss).To(HaveLen(1)) // the open slice survives
		Expect(ss[0].Name).To(Equal(sliceName(3)))
		Expect(ss[0].Sealed).To(BeFalse())
	})

	It("(d) a forced resync with an unchanged source writes nothing", func() {
		create(nil)
		fake.addN(0, 12)
		sync()
		beforeS, beforeRS := rvs(), getRS().ResourceVersion
		for i := 0; i < 3; i++ {
			sync()
		}
		Expect(rvs()).To(Equal(beforeS))
		Expect(getRS().ResourceVersion).To(Equal(beforeRS))
		// NotModified inside the 5 minute window: still nothing.
		fake.notModified = true
		clock = clock.Add(2 * time.Minute)
		sync()
		Expect(getRS().ResourceVersion).To(Equal(beforeRS))
		// After 5 minutes lastSyncTime moves, slices still untouched.
		clock = clock.Add(5 * time.Minute)
		sync()
		Expect(getRS().ResourceVersion).NotTo(Equal(beforeRS))
		Expect(rvs()).To(Equal(beforeS))
	})

	It("(e) every slice stays under 50 KB at N=100 with realistic fields", func() {
		r.MaxReleasesPerSlice = 100
		create(func(rs *kuberikcomv1alpha1.ReleaseSource) { rs.Spec.Retention.Days = ptr.To[int32](0) })
		fake.addN(0, 250)
		sync()
		ss := slices()
		Expect(ss).To(HaveLen(3))
		max := 0
		for _, s := range ss {
			b, err := json.Marshal(s)
			Expect(err).NotTo(HaveOccurred())
			if len(b) > max {
				max = len(b)
			}
			Expect(len(b)).To(BeNumerically("<", 50*1024))
		}
		_, _ = fmt.Printf("MEASURED max slice JSON at N=100: %d bytes (sealed slice of %d releases)\n", max, len(ss[0].Releases))
	})

	It("(f) repairs an unresolved release inside a sealed slice", func() {
		create(nil)
		fake.addN(0, 12)
		sync()
		// An incremental release that fails transiently is admitted as an
		// unresolved stub (its place in the order is fixed by the policy).
		bad := fake.add(12)
		fake.unresolved[bad] = true
		sync()
		fake.addN(13, 30) // pushes it into a sealed slice
		clock = clock.Add(time.Minute)
		sync()
		find := func() (kuberikcomv1alpha1.ReleaseSlice, kuberikcomv1alpha1.Release) {
			for _, sl := range slices() {
				for _, rel := range sl.Releases {
					if rel.Tag == bad {
						return sl, rel
					}
				}
			}
			Fail("tag not found")
			return kuberikcomv1alpha1.ReleaseSlice{}, kuberikcomv1alpha1.Release{}
		}
		sl, rel := find()
		Expect(sl.Sealed).To(BeTrue())
		Expect(rel.MetadataUnresolved).To(BeTrue())
		Expect(rel.CreatedObserved).To(BeTrue())
		seqBefore := rel.Sequence
		before := rvs()
		firstSeen := rel.FirstSeen

		clock = clock.Add(time.Minute)
		sync() // still unresolved: no write
		Expect(rvs()).To(Equal(before))

		delete(fake.unresolved, bad)
		clock = clock.Add(time.Hour) // past any backoff
		sync()
		sl2, got := find()
		Expect(sl2.Name).To(Equal(sl.Name))
		Expect(got.MetadataUnresolved).To(BeFalse())
		Expect(got.Digest).NotTo(BeNil())
		Expect(got.Sequence).To(Equal(seqBefore))
		Expect(got.FirstSeen.Equal(&firstSeen)).To(BeTrue())
		Expect(got.Created.Time.Unix()).To(Equal(baseTime.Add(12 * time.Minute).Unix())) // real created restored
		Expect(sl2.Sealed).To(BeTrue())
		after := rvs()
		for n, rv := range before {
			if n == sl.Name {
				Expect(after[n]).NotTo(Equal(rv))
			} else {
				Expect(after[n]).To(Equal(rv), n)
			}
		}
	})

	It("(g) a deleted newest slice is re-admitted above the maximum without touching other sealed slices", func() {
		create(nil)
		fake.addN(0, 12) // backfill: 00000 = 11..7, 00001 = 6..2, 00002 (open) = 1,0
		sync()
		before := rvs()
		victim := slices()[0]
		Expect(k8sClient.Delete(ctx, &victim)).To(Succeed())
		sync()
		after := rvs()
		Expect(after).NotTo(HaveKey(sliceName(0)))
		Expect(after[sliceName(1)]).To(Equal(before[sliceName(1)]))
		Expect(after[sliceName(2)]).NotTo(Equal(before[sliceName(2)])) // open slice took them
		Expect(getRS().Status.Count).To(Equal(int32(12)))
		seen := map[string]bool{}
		for _, s := range slices() {
			for _, rel := range s.Releases {
				Expect(seen[rel.Tag]).To(BeFalse(), "duplicate "+rel.Tag)
				seen[rel.Tag] = true
			}
		}
		Expect(seen).To(HaveLen(12))
	})

	// QA probe P2: manual slice deletion is unsupported (users cannot write
	// slices), except for the documented rebuild: delete ALL slices and the
	// controller backfills from scratch.
	It("P2 deleting all slices triggers a full backfill", func() {
		create(nil)
		fake.addN(0, 12)
		sync()
		for _, s := range slices() {
			Expect(k8sClient.Delete(ctx, &s)).To(Succeed())
		}
		Expect(slices()).To(BeEmpty())
		resolvesBefore := fake.resolves
		sync()
		Expect(getRS().Status.Count).To(Equal(int32(12)))
		Expect(getRS().Status.BackfillComplete).To(BeTrue())
		Expect(fake.resolves - resolvesBefore).To(Equal(12)) // re-resolved from scratch
		var seqs []int64
		for _, s := range slices() {
			for _, rel := range s.Releases {
				seqs = append(seqs, rel.Sequence)
			}
		}
		Expect(seqs).To(HaveLen(12))
	})

	It("(h) an identity change deletes every slice and rebuilds", func() {
		create(nil)
		fake.addN(0, 12)
		sync()
		old := slices()
		rs := getRS()
		rs.Spec.OCI.Image = "example.com/other"
		Expect(k8sClient.Update(ctx, rs)).To(Succeed())
		sync()
		now := slices()
		Expect(now).To(HaveLen(3))
		for _, o := range old {
			for _, n := range now {
				Expect(n.Name).NotTo(Equal(o.Name))
			}
		}
		Expect(now[0].Annotations).To(HaveKeyWithValue(IdentityAnnotation, "image:example.com/other"))
		Expect(getRS().Status.Count).To(Equal(int32(12)))
		Expect(rec.Events).To(Receive(ContainSubstring("IdentityChanged")))
	})

	It("monotonic admission: policy source drops out-of-order tags, from scratch and after", func() {
		create(nil)
		fake.addN(0, 6)
		// a backport: created later but sorts below the running maximum by tag.
		bp := fakeTag(0) + "-backport"
		fake.mu.Lock()
		fake.tags = append(fake.tags, bp)
		fake.created[bp] = baseTime.Add(10 * time.Minute)
		fake.mu.Unlock()
		sync()
		Expect(getRS().Status.Count).To(Equal(int32(6)))
		resolves := fake.resolves
		sync() // the excluded tag is not re-resolved: the backfill is complete
		Expect(fake.resolves).To(Equal(resolves))
		fake.add(6)
		sync()
		Expect(getRS().Status.Count).To(Equal(int32(7)))
	})

	It("resolve budget leaves Syncing=True and drains over several syncs", func() {
		r.MaxResolvesPerSync = 4
		create(nil)
		fake.addN(0, 10)
		res := sync()
		Expect(res.RequeueAfter).To(BeNumerically("<=", 5*time.Second))
		st := getRS().Status
		Expect(meta.IsStatusConditionTrue(st.Conditions, kuberikcomv1alpha1.SyncingCondition)).To(BeTrue())
		Expect(st.Count).To(Equal(int32(4)))
		sync()
		sync()
		st = getRS().Status
		Expect(meta.IsStatusConditionTrue(st.Conditions, kuberikcomv1alpha1.SyncingCondition)).To(BeFalse())
		Expect(st.Count).To(Equal(int32(10)))
	})

	It("created falls back to firstSeen with createdObserved", func() {
		create(nil)
		t := fake.add(1)
		fake.created[t] = time.Time{}
		sync()
		rel := slices()[0].Releases[0]
		Expect(rel.CreatedObserved).To(BeTrue())
		Expect(rel.Created.Equal(&rel.FirstSeen)).To(BeTrue())
		Expect(rel.FirstSeen.Time.Unix()).To(Equal(clock.Unix()))
	})

	It("unreachable keeps the catalogue: Stale, then Ready=False after 3 intervals", func() {
		create(nil)
		fake.addN(0, 3)
		sync()
		fake.err = errors.New("registry down")
		sync()
		st := getRS().Status
		reach := meta.FindStatusCondition(st.Conditions, kuberikcomv1alpha1.SourceReachableCondition)
		ready := meta.FindStatusCondition(st.Conditions, kuberikcomv1alpha1.ReadyCondition)
		Expect(reach.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		Expect(ready.Reason).To(Equal("Stale"))
		Expect(st.Count).To(Equal(int32(3)))
		clock = clock.Add(4 * time.Minute)
		sync()
		ready = meta.FindStatusCondition(getRS().Status.Conditions, kuberikcomv1alpha1.ReadyCondition)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(slices()).To(HaveLen(1))
	})

	It("bad config sets Ready=False and writes no slices; suspend does nothing", func() {
		r.SourceFactory = func(context.Context, client.Reader, *kuberikcomv1alpha1.ReleaseSource) (source.Source, error) {
			return nil, errors.New("imagePolicyRef not found")
		}
		create(nil)
		sync()
		ready := meta.FindStatusCondition(getRS().Status.Conditions, kuberikcomv1alpha1.ReadyCondition)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("ConfigInvalid"))
		Expect(slices()).To(BeEmpty())

		r.SourceFactory = func(context.Context, client.Reader, *kuberikcomv1alpha1.ReleaseSource) (source.Source, error) {
			return fake, nil
		}
		rs := getRS()
		rs.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, rs)).To(Succeed())
		fake.addN(0, 3)
		res := sync()
		Expect(res.RequeueAfter).To(BeZero())
		Expect(slices()).To(BeEmpty())
	})

	// QA probe P1 (adapted): a resolve budget under a policy must not lose
	// anything; every tag in the window is admitted across syncs, in policy order.
	It("P1 resolve budget under a policy admits everything across syncs, sequences follow the policy", func() {
		r.MaxResolvesPerSync = 4
		create(func(rs *kuberikcomv1alpha1.ReleaseSource) { rs.Spec.Retention.Days = ptr.To[int32](0) })
		fake.addN(0, 30)
		syncs := 0
		for ; syncs < 20; syncs++ {
			sync()
			if getRS().Status.Count == 30 {
				break
			}
		}
		Expect(getRS().Status.Count).To(Equal(int32(30)))
		Expect(getRS().Status.BackfillComplete).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(getRS().Status.Conditions, kuberikcomv1alpha1.SyncingCondition)).To(BeFalse())
		var all []kuberikcomv1alpha1.Release
		for _, sl := range slices() {
			all = append(all, sl.Releases...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].Sequence < all[j].Sequence })
		for i := range all {
			Expect(all[i].Tag).To(Equal(fakeTag(i)), "sequence order must equal policy order")
			if i > 0 {
				Expect(all[i].Sequence).To(BeNumerically(">", all[i-1].Sequence))
			}
		}
		Expect(getRS().Status.Newest.Tag).To(Equal(fakeTag(29)))
		// the same under incremental admission: 10 new tags, budget 4
		fake.addN(30, 40)
		for i := 0; i < 10 && getRS().Status.Count < 40; i++ {
			sync()
		}
		Expect(getRS().Status.Count).To(Equal(int32(40)))
		Expect(getRS().Status.Newest.Tag).To(Equal(fakeTag(39)))
	})

	// QA probe P3 (adapted): a failed create after the sealing update must not
	// lose releases; they are rediscovered above the maximum.
	It("P3 a failed create after the seal loses no release", func() {
		fc := &failCreate{Client: k8sClient}
		r.Client = fc
		create(func(rs *kuberikcomv1alpha1.ReleaseSource) { rs.Spec.Retention.Days = ptr.To[int32](0) })
		fake.addN(0, 4)
		sync() // one open slice with 4
		fake.addN(4, 12)
		fc.fail = true
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
		Expect(err).To(HaveOccurred())
		for i := 0; i < 4; i++ {
			clock = clock.Add(time.Minute)
			sync()
		}
		Expect(getRS().Status.Count).To(Equal(int32(12)))
		seen := map[string]bool{}
		for _, s := range slices() {
			for _, rel := range s.Releases {
				Expect(seen[rel.Tag]).To(BeFalse())
				seen[rel.Tag] = true
			}
		}
		Expect(seen).To(HaveLen(12))
	})

	// QA probe P4 (adapted): tags that never resolve must not hot-loop at 1 s.
	It("P4 permanently failing tags back off instead of looping at one second", func() {
		r.MaxResolvesPerSync = 4
		create(nil)
		fake.addN(0, 6)
		for i := 0; i < 6; i++ {
			fake.unresolved[fakeTag(i)] = true
		}
		var requeues []time.Duration
		for i := 0; i < 8; i++ {
			res := sync()
			requeues = append(requeues, res.RequeueAfter)
			clock = clock.Add(res.RequeueAfter) // the controller is woken exactly then
		}
		for i, d := range requeues {
			Expect(d).To(BeNumerically(">=", 9*time.Second), "requeue %d = %v", i, d)
			if i > 0 && i < 4 {
				Expect(d).To(BeNumerically(">=", requeues[i-1]), "backoff must not shrink: %v", requeues)
			}
		}
		// one resolve per due retry, not one per second
		Expect(fake.resolves).To(BeNumerically("<=", 10))
		ready := meta.FindStatusCondition(getRS().Status.Conditions, kuberikcomv1alpha1.ReadyCondition)
		Expect(ready.Status).To(Equal(metav1.ConditionTrue))
	})

	It("a restarted reconciler costs no resolves for tags below the maximum", func() {
		create(nil)
		fake.addN(0, 30)
		sync()
		Expect(getRS().Status.BackfillComplete).To(BeTrue())
		fresh := &ReleaseSourceReconciler{Client: r.Client, Scheme: r.Scheme, MaxReleasesPerSlice: r.MaxReleasesPerSlice, MaxSliceBytes: r.MaxSliceBytes,
			MaxResolvesPerSync: r.MaxResolvesPerSync, Recorder: rec, Now: r.Now, SourceFactory: r.SourceFactory} // new process: no in-memory state
		fake.resolves = 0
		_, err := fresh.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
		Expect(err).NotTo(HaveOccurred())
		Expect(fake.resolves).To(BeZero())
		fake.add(30)
		_, err = fresh.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
		Expect(err).NotTo(HaveOccurred())
		Expect(fake.resolves).To(Equal(1)) // only the new tag
	})

	It("a bounds rejection is stored once, evented once and never retried", func() {
		create(nil)
		fake.addN(0, 3)
		bad := fake.add(3)
		fake.rejects[bad] = "requires has 33 keys"
		fake.addN(4, 6)
		sync()
		var got kuberikcomv1alpha1.Release
		for _, sl := range slices() {
			for _, rel := range sl.Releases {
				if rel.Tag == bad {
					got = rel
				}
			}
		}
		Expect(got.MetadataUnresolved).To(BeTrue())
		Expect(got.Metadata).To(HaveKeyWithValue("kuberik.com/rejected", "requires has 33 keys"))
		events := 0
		for len(rec.Events) > 0 {
			if e := <-rec.Events; strings.Contains(e, "MetadataRejected") {
				events++
			}
		}
		Expect(events).To(Equal(1))
		resolves, rv := fake.resolves, rvs()
		for i := 0; i < 3; i++ {
			clock = clock.Add(time.Hour)
			sync()
		}
		Expect(fake.resolves).To(Equal(resolves))
		Expect(rvs()).To(Equal(rv))
		Expect(rec.Events).To(BeEmpty())
	})

	It("TagsSkipped is evented only when the count changes", func() {
		create(nil)
		fake.addN(0, 3)
		fake.skippedLong = 2
		sync()
		sync()
		fake.skippedLong = 3
		sync()
		sync()
		n := 0
		for len(rec.Events) > 0 {
			if e := <-rec.Events; strings.Contains(e, "TagsSkipped") {
				n++
			}
		}
		Expect(n).To(Equal(2))
	})

	It("an identity change keeps the old catalogue when the new source cannot be listed, and includes the resolved image", func() {
		create(func(rs *kuberikcomv1alpha1.ReleaseSource) {
			rs.Spec.OCI = &kuberikcomv1alpha1.OCISource{ImagePolicyRef: &corev1.LocalObjectReference{Name: "pol"}}
		})
		fake.ident = "ghcr.io/x/app"
		fake.addN(0, 6)
		sync()
		Expect(slices()[0].Annotations).To(HaveKeyWithValue(IdentityAnnotation, "imagePolicyRef:pol@ghcr.io/x/app"))
		before := rvs()

		// the ImageRepository now points elsewhere, and that registry is down
		fake.ident = "ghcr.io/x/other"
		fake.err = errors.New("registry down")
		sync()
		Expect(rvs()).To(Equal(before))
		Expect(getRS().Status.Count).To(Equal(int32(6)))

		// recovered: rebuilt from the new image
		fake.err = nil
		sync()
		ss := slices()
		Expect(ss).NotTo(BeEmpty())
		Expect(ss[0].Annotations).To(HaveKeyWithValue(IdentityAnnotation, "imagePolicyRef:pol@ghcr.io/x/other"))
		for n := range before {
			Expect(rvs()).NotTo(HaveKey(n))
		}
	})
})

type failCreate struct {
	client.Client
	fail bool
}

func (f *failCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if f.fail {
		f.fail = false
		return errors.New("injected: create failed (crash / 500 / quota)")
	}
	return f.Client.Create(ctx, obj, opts...)
}
