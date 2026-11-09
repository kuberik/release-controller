package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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

// sweepClient fails the k-th write (slice Update/Create/Delete, status Update)
// counted after arm. It also logs the write kinds.
type sweepClient struct {
	client.Client
	armed  bool
	n      int
	failAt int
	ops    []string
	stale  *kuberikcomv1alpha1.ReleaseSliceList
}

func (c *sweepClient) hit(op string) error {
	if !c.armed {
		return nil
	}
	c.n++
	c.ops = append(c.ops, op)
	if c.failAt > 0 && c.n == c.failAt {
		return apierrors.NewServerTimeout(schema.GroupResource{Group: "kuberik.com", Resource: "releaseslices"}, op, 1)
	}
	return nil
}
func (c *sweepClient) Update(ctx context.Context, obj client.Object, o ...client.UpdateOption) error {
	if _, ok := obj.(*kuberikcomv1alpha1.ReleaseSlice); ok {
		if err := c.hit("U"); err != nil {
			return err
		}
	}
	return c.Client.Update(ctx, obj, o...)
}
func (c *sweepClient) Create(ctx context.Context, obj client.Object, o ...client.CreateOption) error {
	if _, ok := obj.(*kuberikcomv1alpha1.ReleaseSlice); ok {
		if err := c.hit("C"); err != nil {
			return err
		}
	}
	return c.Client.Create(ctx, obj, o...)
}
func (c *sweepClient) Delete(ctx context.Context, obj client.Object, o ...client.DeleteOption) error {
	if _, ok := obj.(*kuberikcomv1alpha1.ReleaseSlice); ok {
		if err := c.hit("D"); err != nil {
			return err
		}
	}
	return c.Client.Delete(ctx, obj, o...)
}
func (c *sweepClient) List(ctx context.Context, l client.ObjectList, o ...client.ListOption) error {
	if sl, ok := l.(*kuberikcomv1alpha1.ReleaseSliceList); ok && c.stale != nil {
		c.stale.DeepCopyInto(sl)
		return nil
	}
	return c.Client.List(ctx, l, o...)
}

type sweepStatus struct {
	client.SubResourceWriter
	c *sweepClient
}

func (s sweepStatus) Update(ctx context.Context, obj client.Object, o ...client.SubResourceUpdateOption) error {
	if err := s.c.hit("S"); err != nil {
		return err
	}
	return s.SubResourceWriter.Update(ctx, obj, o...)
}
func (c *sweepClient) Status() client.SubResourceWriter {
	return sweepStatus{c.Client.Status(), c}
}

var _ = Describe("crash sweep, backfill stubs, sequence collision", func() {
	const ns = "default"
	ctx := context.Background()
	var (
		fake  *fakeSource
		r     *ReleaseSourceReconciler
		sc    *sweepClient
		name  string
		n     int
		clock time.Time
	)
	key := func() types.NamespacedName { return types.NamespacedName{Name: name, Namespace: ns} }
	sync := func() (reconcile.Result, error) {
		return r.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
	}
	list := func() []kuberikcomv1alpha1.ReleaseSlice {
		var l kuberikcomv1alpha1.ReleaseSliceList
		Expect(k8sClient.List(ctx, &l, client.InNamespace(ns), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: name})).To(Succeed())
		sort.Slice(l.Items, func(i, j int) bool { return l.Items[i].Name < l.Items[j].Name })
		return l.Items
	}
	// snapshot: tag -> sequence, plus integrity counters.
	snap := func() (m map[string]int64, dupSeq, disorder, twice int) {
		cat := slicer.Merge(list())
		m = map[string]int64{}
		seqs := map[int64]int{}
		for i, c := range cat {
			m[c.Tag] = c.Sequence
			seqs[c.Sequence]++
			if i > 0 && cat[i-1].Tag > c.Tag {
				disorder++
			}
		}
		for _, v := range seqs {
			if v > 1 {
				dupSeq++
			}
		}
		stored := 0
		for _, s := range list() {
			stored += len(s.Releases)
		}
		twice = stored - len(cat)
		return
	}
	tagIdx := func(t string) string { return strings.TrimLeft(t[5:15], "0") }
	diff := func(want, got map[string]int64) string {
		var miss, extra, reseq []string
		for t, s := range want {
			g, ok := got[t]
			switch {
			case !ok:
				miss = append(miss, tagIdx(t))
			case g != s:
				reseq = append(reseq, fmt.Sprintf("%s:%d->%d", tagIdx(t), s, g))
			}
		}
		for t := range got {
			if _, ok := want[t]; !ok {
				extra = append(extra, tagIdx(t))
			}
		}
		sort.Strings(miss)
		sort.Strings(extra)
		sort.Strings(reseq)
		if len(miss)+len(extra)+len(reseq) == 0 {
			return ""
		}
		return fmt.Sprintf("missing=%v extra=%v reseq=%v", miss, extra, reseq)
	}
	newRS := func(days int32, minCount int32) {
		n++
		name = fmt.Sprintf("qa4-%d", n)
		rs := &kuberikcomv1alpha1.ReleaseSource{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: kuberikcomv1alpha1.ReleaseSourceSpec{
				OCI:       &kuberikcomv1alpha1.OCISource{Image: "example.com/app", Policy: &kuberikcomv1alpha1.OrderPolicy{Alphabetical: &kuberikcomv1alpha1.AlphabeticalPolicy{}}},
				Retention: kuberikcomv1alpha1.Retention{Days: ptr.To(days), MinCount: ptr.To(minCount)},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
	}
	newR := func(perSlice int) {
		sc = &sweepClient{Client: k8sClient}
		r = &ReleaseSourceReconciler{
			Client: sc, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			MaxReleasesPerSlice: perSlice, MaxSliceBytes: 256 * 1024, MaxResolvesPerSync: 10,
			Recorder: record.NewFakeRecorder(100000),
			Now:      func() time.Time { return clock },
			SourceFactory: func(context.Context, client.Reader, *kuberikcomv1alpha1.ReleaseSource) (source.Source, error) {
				return fake, nil
			},
		}
	}
	// settle runs syncs, 1 s apart, ignoring errors; returns the error count.
	settle := func(k int) int {
		errs := 0
		for i := 0; i < k; i++ {
			if _, err := sync(); err != nil {
				errs++
			}
			clock = clock.Add(time.Second)
		}
		return errs
	}

	type scenario struct {
		name     string
		perSlice int
		days     int32
		minCount int32
		setup    func() // before arming
		phase    func() // armed: the phase under test (syncs, ignores errors)
		after    func() // recovery
	}
	createdEvery := func(d time.Duration) {
		for i := range fake.tags {
			fake.created[fake.tags[i]] = baseTime.Add(time.Duration(i) * d)
		}
	}
	// phase helper: k syncs; on a failed sync in crash mode, drop memory.
	var crash bool
	phaseSyncs := func(k int) {
		for i := 0; i < k; i++ {
			_, err := sync()
			if err != nil && crash {
				r.mem = nil
			}
			clock = clock.Add(time.Second)
		}
	}
	scenarios := []scenario{
		{name: "backfill/perSlice7", perSlice: 7, days: 0, minCount: 3,
			setup: func() { fake.addN(0, 40) },
			phase: func() { phaseSyncs(4) },
			after: func() { settle(12) }},
		{name: "backfill/perSlice100", perSlice: 100, days: 0, minCount: 3,
			setup: func() { fake.addN(0, 40) },
			phase: func() { phaseSyncs(4) },
			after: func() { settle(12) }},
		{name: "incremental/perSlice7", perSlice: 7, days: 0, minCount: 3,
			setup: func() { fake.addN(0, 12); settle(4) },
			phase: func() {
				fake.addN(12, 20)
				phaseSyncs(1)
				fake.addN(20, 33)
				phaseSyncs(2)
				fake.addN(33, 36)
				phaseSyncs(1)
			},
			after: func() { settle(8) }},
		{name: "retention/perSlice5", perSlice: 5, days: 7, minCount: 3,
			setup: func() {
				fake.addN(0, 40)
				createdEvery(12 * time.Hour)
				clock = fake.created[fakeTag(39)].Add(time.Hour)
				settle(8)
			},
			phase: func() {
				for i := 40; i < 60; i++ {
					fake.add(i)
					fake.created[fakeTag(i)] = baseTime.Add(time.Duration(i) * 12 * time.Hour)
					clock = fake.created[fakeTag(i)].Add(time.Hour)
					phaseSyncs(1)
				}
			},
			after: func() { settle(8) }},
		{name: "identity-rebuild/perSlice7", perSlice: 7, days: 0, minCount: 3,
			setup: func() { fake.addN(0, 40); settle(8) },
			phase: func() { fake.fp = "v2"; phaseSyncs(6) },
			after: func() { settle(12) }},
	}

	It("crash-point sweep", func() {
		total := 0
		var bad []string
		for _, s := range scenarios {
			// control run
			fake = newFake()
			clock = baseTime.Add(time.Hour)
			crash = false
			newRS(s.days, s.minCount)
			newR(s.perSlice)
			s.setup()
			sc.armed = true
			s.phase()
			writes := sc.n
			ops := strings.Join(sc.ops, "")
			s.after()
			want, d0, o0, t0 := snap()
			_, _ = fmt.Printf("QA4 %s control: writes=%d ops=%s count=%d dup=%d disorder=%d twice=%d\n", s.name, writes, ops, len(want), d0, o0, t0)
			for _, mode := range []bool{false, true} {
				for k := 1; k <= writes; k++ {
					fake = newFake()
					clock = baseTime.Add(time.Hour)
					crash = mode
					newRS(s.days, s.minCount)
					newR(s.perSlice)
					s.setup()
					sc.armed, sc.failAt = true, k
					s.phase()
					op := "?"
					if k <= len(sc.ops) {
						op = sc.ops[k-1]
					}
					sc.failAt = 0
					s.after()
					got, d, o, tw := snap()
					total++
					df := diff(want, got)
					if df != "" || d+o+tw > 0 {
						line := fmt.Sprintf("%s crash=%v k=%d op=%s: count=%d dup=%d disorder=%d twice=%d %s", s.name, mode, k, op, len(got), d, o, tw, df)
						bad = append(bad, line)
						_, _ = fmt.Printf("QA4 BAD %s\n", line)
					}
				}
			}
		}
		_, _ = fmt.Printf("QA4 sweep cases=%d bad=%d\n", total, len(bad))
		Expect(total).To(BeNumerically(">=", 192))
		Expect(bad).To(BeEmpty())
	})

	It("stub admitted after 5 attempts vs a backport (created rule)", func() {
		run := func(fail bool) map[string]int64 {
			fake = newFake()
			clock = baseTime.Add(2 * time.Hour)
			newRS(0, 3)
			newR(10)
			fake.addN(0, 40)
			// tag 25 is a backport: built after everything above it.
			fake.created[fakeTag(25)] = baseTime.Add(100 * time.Minute)
			_, err := sync() // 39..30
			Expect(err).NotTo(HaveOccurred())
			if fail {
				fake.unresolved[fakeTag(25)] = true
			}
			for i := 0; i < 40; i++ {
				res, err := sync()
				Expect(err).NotTo(HaveOccurred())
				if i == 20 {
					fake.unresolved[fakeTag(25)] = false
				}
				d := res.RequeueAfter
				if d > time.Hour {
					d = time.Hour
				}
				clock = clock.Add(d)
			}
			m, d, o, tw := snap()
			_, _ = fmt.Printf("QA4 STUB fail=%v count=%d dup=%d disorder=%d twice=%d has25=%v\n", fail, len(m), d, o, tw, m[fakeTag(25)] != 0)
			return m
		}
		want := run(false)
		got := run(true)
		_, _ = fmt.Printf("QA4 STUB diff control->failing: %s\n", diff(want, got))
		Expect(diff(want, got)).To(BeEmpty())
		Expect(got).NotTo(HaveKey(fakeTag(25)))
		rec := r.Recorder.(*record.FakeRecorder)
		unres := 0
		for len(rec.Events) > 0 {
			if strings.Contains(<-rec.Events, "TagUnresolvable") {
				unres++
			}
		}
		Expect(unres).To(Equal(1), "TagUnresolvable fires once")
	})

	It("stub of a legit tag lets a later-built lower tag in", func() {
		run := func(fail bool) map[string]int64 {
			fake = newFake()
			clock = baseTime.Add(2 * time.Hour)
			newRS(0, 3)
			newR(10)
			fake.addN(0, 40)
			// tag 24 built 30 s after 25 (a quick backport): the rule excludes it.
			fake.created[fakeTag(24)] = baseTime.Add(25*time.Minute + 30*time.Second)
			_, err := sync()
			Expect(err).NotTo(HaveOccurred())
			if fail {
				fake.unresolved[fakeTag(25)] = true
			}
			for i := 0; i < 40; i++ {
				res, err := sync()
				Expect(err).NotTo(HaveOccurred())
				if i == 20 {
					fake.unresolved[fakeTag(25)] = false
				}
				d := res.RequeueAfter
				if d > time.Hour {
					d = time.Hour
				}
				clock = clock.Add(d)
			}
			m, _, _, _ := snap()
			_, _ = fmt.Printf("QA4 STUB2 fail=%v count=%d has24=%v\n", fail, len(m), m[fakeTag(24)] != 0)
			return m
		}
		want := run(false)
		got := run(true)
		_, _ = fmt.Printf("QA4 STUB2 diff control->failing: %s\n", diff(want, got))
		// The unresolvable tag is excluded. Its created time is unknowable, so
		// the quick backport below it is then judged against tag 26 and gets in:
		// the one documented residue of excluding instead of stubbing.
		Expect(got).NotTo(HaveKey(fakeTag(25)))
		Expect(want).To(HaveKey(fakeTag(25)))
	})

	It("CheckSequences backstop without APIReader (stale list reaches admission)", func() {
		fake = newFake()
		clock = baseTime.Add(time.Hour)
		newRS(0, 3)
		newR(10)
		r.APIReader = nil
		fake.addN(0, 12)
		settle(3)
		before := list()
		fake.addN(12, 22)
		settle(1)
		sc.stale = &kuberikcomv1alpha1.ReleaseSliceList{Items: before}
		fake.addN(22, 25)
		_, err := sync()
		_, _ = fmt.Printf("QA4 NOAPI stale sync err=%v\n", err)
		sc.stale = nil
		errs := settle(4)
		m, d, o, tw := snap()
		_, _ = fmt.Printf("QA4 NOAPI after: count=%d dup=%d disorder=%d twice=%d errs=%d\n", len(m), d, o, tw, errs)
	})

	It("a colliding catalogue wedges the source: surfaced?", func() {
		fake = newFake()
		clock = baseTime.Add(time.Hour)
		newRS(0, 3)
		newR(10)
		fake.addN(0, 12)
		settle(3)
		sl := list()
		// corrupt: give the newest release of the last slice the sequence of its neighbour
		last := &sl[len(sl)-1]
		rels := last.Releases
		Expect(len(rels)).To(BeNumerically(">=", 2))
		rels[len(rels)-1].Sequence = rels[len(rels)-2].Sequence
		Expect(k8sClient.Update(ctx, last)).To(Succeed())
		fake.addN(12, 15)
		errs := settle(5)
		rs := &kuberikcomv1alpha1.ReleaseSource{}
		Expect(k8sClient.Get(ctx, key(), rs)).To(Succeed())
		var conds []string
		for _, c := range rs.Status.Conditions {
			conds = append(conds, fmt.Sprintf("%s=%s/%s", c.Type, c.Status, c.Reason))
		}
		m, _, _, _ := snap()
		var evs []string
		rec := r.Recorder.(*record.FakeRecorder)
		for len(rec.Events) > 0 {
			evs = append(evs, <-rec.Events)
		}
		_, _ = fmt.Printf("QA4 WEDGE errs=%d/5 count=%d (want 15) conds=%v events=%v\n", errs, len(m), conds, evs)
		Expect(errs).To(Equal(0))
		Expect(len(m)).To(Equal(12), "no writes while the collision stands")
		ready := meta.FindStatusCondition(rs.Status.Conditions, kuberikcomv1alpha1.ReadyCondition)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("SequenceCollision"))
		Expect(ready.Message).To(ContainSubstring("is held by both"))
		collisionEvents := 0
		for _, e := range evs {
			if strings.Contains(e, "SequenceCollision") {
				collisionEvents++
			}
		}
		Expect(collisionEvents).To(Equal(1), "one Warning on the transition")
	})
})
