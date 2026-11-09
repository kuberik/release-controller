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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

// fakeSource is a controllable source. Policy mode orders by tag (the tags
// are zero-padded, so lexical is numeric); admitAll skips admission.
type fakeSource struct {
	mu          sync.Mutex
	tags        []string
	created     map[string]time.Time // zero or missing: no created annotation
	unresolved  map[string]bool
	err         error
	notModified bool
	admitAll    bool
	resolves    int
}

func newFake() *fakeSource {
	return &fakeSource{created: map[string]time.Time{}, unresolved: map[string]bool{}}
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
	if f.unresolved[ref.Tag] {
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
func (f *fakeSource) AdmitAll() bool                { return f.admitAll }

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
				OCI:       &kuberikcomv1alpha1.OCISource{Image: "example.com/app"},
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
		fake.admitAll = true
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
		Expect(st.Slices).To(Equal([]string{sliceName(0), sliceName(1), sliceName(2)}))
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

	It("(c) retention drops whole oldest sealed slices and respects minCount", func() {
		create(nil)
		fake.addN(0, 12) // 5,5,2
		sync()
		clock = baseTime.Add(30 * 24 * time.Hour)
		sync() // minCount 3: slice 0 goes (7 left), slice 1 would leave 2
		ss := slices()
		Expect(ss).To(HaveLen(2))
		Expect(ss[0].Name).To(Equal(sliceName(1)))
		Expect(getRS().Status.Count).To(Equal(int32(7)))

		rs := getRS()
		rs.Spec.Retention.MinCount = ptr.To[int32](0)
		Expect(k8sClient.Update(ctx, rs)).To(Succeed())
		sync()
		ss = slices()
		Expect(ss).To(HaveLen(1))
		Expect(ss[0].Sealed).To(BeFalse()) // open slice never deleted
		Expect(ss[0].Name).To(Equal(sliceName(2)))
		Expect(getRS().Status.Slices).To(Equal([]string{sliceName(2)}))
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
		bad := fakeTag(2)
		fake.unresolved[bad] = true
		sync()
		// An unresolved release has no created, so created = firstSeen = now
		// and it sorts after the 12. Later releases push it into a sealed slice.
		fake.addN(200, 215)
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
		before := rvs()
		firstSeen := rel.FirstSeen

		clock = clock.Add(time.Minute)
		sync() // still unresolved: no write
		Expect(rvs()).To(Equal(before))

		delete(fake.unresolved, bad)
		clock = clock.Add(time.Minute)
		sync()
		sl2, got := find()
		Expect(sl2.Name).To(Equal(sl.Name))
		Expect(got.MetadataUnresolved).To(BeFalse())
		Expect(got.Digest).NotTo(BeNil())
		Expect(got.FirstSeen.Equal(&firstSeen)).To(BeTrue())
		Expect(got.Created.Time.Unix()).To(Equal(baseTime.Add(2 * time.Minute).Unix())) // real created restored
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

	It("(g) a deleted slice is re-filled into the open slice without touching sealed ones", func() {
		create(nil)
		fake.addN(0, 12) // 5,5,2
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
		fake.admitAll = false
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
		sync() // skipped tag is not re-resolved
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
})
