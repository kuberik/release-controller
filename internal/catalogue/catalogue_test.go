package catalogue

import (
	"fmt"
	"sort"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/source"
)

var day0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// reg is a fake registry: tags order lexically ("t01" < "t02"); created is per tag.
type reg struct {
	created   map[string]time.Time // zero or missing: no created annotation
	transient map[string]bool
	reject    map[string]string
	resolved  []string
}

func newReg(n int) *reg {
	r := &reg{created: map[string]time.Time{}, transient: map[string]bool{}, reject: map[string]string{}}
	for i := 1; i <= n; i++ {
		r.created[tag(i)] = day0.Add(time.Duration(i) * 24 * time.Hour) // one tag per day
	}
	return r
}

func tag(i int) string { return fmt.Sprintf("t%03d", i) }

func (r *reg) refs() []source.Ref {
	var out []source.Ref
	for t := range r.created {
		out = append(out, source.Ref{Tag: t})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag > out[j].Tag }) // listing order is irrelevant
	return out
}

func (r *reg) resolve(ref source.Ref) Resolved {
	r.resolved = append(r.resolved, ref.Tag)
	rel := source.Release{Tag: ref.Tag}
	if r.transient[ref.Tag] {
		rel.MetadataUnresolved = true
		return Resolved{Release: rel}
	}
	if m, ok := r.reject[ref.Tag]; ok {
		rel.MetadataUnresolved = true
		return Resolved{Release: rel, Rejected: m}
	}
	if c := r.created[ref.Tag]; !c.IsZero() {
		rel.Created = metav1.NewTime(c)
	}
	return Resolved{Release: rel}
}

func less(a, b source.Release) bool { return a.Tag < b.Tag }

func (r *reg) cfg(now time.Time, days int32, minCount, budget int) Config {
	return Config{Less: less, Resolve: r.resolve, Budget: budget, Days: days, MinCount: minCount, Now: now}
}

// apply merges a result into the state like the slices would hold it.
func apply(st State, res Result) State {
	st.Catalogue = append(append([]v1alpha1.Release(nil), st.Catalogue...), res.Admitted...)
	sort.Slice(st.Catalogue, func(i, j int) bool { return st.Catalogue[i].Sequence < st.Catalogue[j].Sequence })
	st.BackfillComplete = res.BackfillComplete
	return st
}

func tagsOf(st State) (out []string) {
	for _, c := range st.Catalogue {
		out = append(out, c.Tag)
	}
	return
}

func TestIncrementalIsTagOnlyAndNumbersFromMax(t *testing.T) {
	r := newReg(10)
	st := State{BackfillComplete: true}
	for i := 5; i <= 7; i++ {
		st.Catalogue = append(st.Catalogue, v1alpha1.Release{Tag: tag(i), Sequence: int64(100 + i)})
	}
	res := Admit(st, r.refs(), r.cfg(day0.Add(20*24*time.Hour), 7, 3, 50))
	if fmt.Sprint(r.resolved) != "[t008 t009 t010]" { // oldest first; t01..t04 below the maximum never resolved
		t.Fatalf("resolved %v", r.resolved)
	}
	if len(res.Admitted) != 3 || res.Admitted[0].Sequence != 108 || res.Admitted[2].Sequence != 110 || res.Admitted[2].Tag != "t010" {
		t.Fatalf("admitted %+v", res.Admitted)
	}
}

func TestIncrementalPartialBatchAdmitsOldestFirst(t *testing.T) {
	r := newReg(10)
	st := State{BackfillComplete: true, Catalogue: []v1alpha1.Release{{Tag: tag(5), Sequence: 5}}}
	res := Admit(st, r.refs(), r.cfg(day0.Add(20*24*time.Hour), 7, 3, 2))
	if len(res.Admitted) != 2 || res.Admitted[0].Tag != "t006" || res.Admitted[1].Tag != "t007" || res.Leftover != 3 {
		t.Fatalf("admitted %+v leftover %d", res.Admitted, res.Leftover)
	}
	// the next sync continues from the new maximum
	st = apply(st, res)
	res = Admit(st, r.refs(), r.cfg(day0.Add(20*24*time.Hour), 7, 3, 10))
	if len(res.Admitted) != 3 || res.Admitted[0].Tag != "t008" || res.Admitted[0].Sequence != 8 || res.Leftover != 0 {
		t.Fatalf("second batch %+v", res.Admitted)
	}
}

func TestBackfillWalksDescendingAndStopsPastWindow(t *testing.T) {
	r := newReg(30)
	now := day0.Add(30 * 24 * time.Hour) // t030 created day 30: window 7 days -> t023 is the first past (created day 23 < cutoff day 23?)
	res := Admit(State{}, r.refs(), r.cfg(now, 7, 3, 100))
	if !res.BackfillComplete {
		t.Fatal("expected complete")
	}
	first, last := res.Admitted[0], res.Admitted[len(res.Admitted)-1]
	if first.Tag != "t030" || first.Sequence != SequenceOrigin {
		t.Fatalf("first %+v", first)
	}
	// cutoff = now-7d = day 23 exactly; created < cutoff means t022.
	if last.Tag != "t022" || last.Sequence != SequenceOrigin-8 {
		t.Fatalf("last %s seq %d", last.Tag, last.Sequence)
	}
	if len(r.resolved) != 9 {
		t.Fatalf("resolved %d tags, the walk must stop at the window, not list-length", len(r.resolved))
	}
	for i := 1; i < len(res.Admitted); i++ {
		if res.Admitted[i].Sequence != res.Admitted[i-1].Sequence-1 {
			t.Fatal("sequences must descend by one")
		}
	}
}

func TestBackfillNeedsMinCountEvenPastWindow(t *testing.T) {
	r := newReg(30)
	now := day0.Add(60 * 24 * time.Hour) // everything is expired
	res := Admit(State{}, r.refs(), r.cfg(now, 7, 5, 100))
	if len(res.Admitted) != 5 || !res.BackfillComplete {
		t.Fatalf("admitted %d complete %v", len(res.Admitted), res.BackfillComplete)
	}
}

func TestBackfillDaysZeroWalksEverything(t *testing.T) {
	r := newReg(30)
	res := Admit(State{}, r.refs(), r.cfg(day0.Add(60*24*time.Hour), 0, 3, 100))
	if len(res.Admitted) != 30 || !res.BackfillComplete {
		t.Fatalf("admitted %d", len(res.Admitted))
	}
}

// A tag created later than a tag above it (a retag, a backport) is excluded.
func TestBackfillCreatedRuleExcludesRetag(t *testing.T) {
	r := newReg(10)
	r.created[tag(6)] = day0.Add(50 * 24 * time.Hour) // re-pushed long after t007..t010
	res := Admit(State{}, r.refs(), r.cfg(day0.Add(12*24*time.Hour), 0, 3, 100))
	for _, a := range res.Admitted {
		if a.Tag == tag(6) {
			t.Fatal("retag admitted")
		}
	}
	if len(res.Admitted) != 9 {
		t.Fatalf("admitted %d", len(res.Admitted))
	}
	// equal created is admitted ("not later than")
	r2 := newReg(3)
	r2.created[tag(2)] = r2.created[tag(3)]
	if got := Admit(State{}, r2.refs(), r2.cfg(day0.Add(9*24*time.Hour), 0, 1, 100)); len(got.Admitted) != 3 {
		t.Fatalf("equal created must be admitted: %d", len(got.Admitted))
	}
}

// Releases without created are admitted by position, neither rejected nor
// tightening the rule, and never trigger the window stop.
func TestBackfillNoCreatedAdmittedByPosition(t *testing.T) {
	r := newReg(10)
	r.created[tag(8)] = time.Time{}
	r.created[tag(5)] = time.Time{}
	res := Admit(State{}, r.refs(), r.cfg(day0.Add(12*24*time.Hour), 7, 3, 100)) // cutoff day 5
	got := map[string]v1alpha1.Release{}
	for _, a := range res.Admitted {
		got[a.Tag] = a
	}
	if !got[tag(8)].CreatedObserved || !got[tag(5)].CreatedObserved {
		t.Fatal("no created => createdObserved")
	}
	// t005 has no created (day 5 would not count as past the cutoff anyway);
	// the walk goes on to t004 (day 4 < cutoff), the first real created past it.
	if last := res.Admitted[len(res.Admitted)-1]; last.Tag != tag(4) || len(res.Admitted) != 7 {
		t.Fatalf("last %s of %d", last.Tag, len(res.Admitted))
	}
}

func TestBackfillResumesAcrossSyncsLikeOneBigRun(t *testing.T) {
	r := newReg(60)
	now := day0.Add(60 * 24 * time.Hour)
	big := Admit(State{}, r.refs(), r.cfg(now, 7, 3, 1000))
	want := State{}
	want = apply(want, big)

	var st State
	syncs := 0
	for ; syncs < 50; syncs++ {
		res := Admit(st, r.refs(), r.cfg(now, 7, 3, 4))
		st = apply(st, res)
		if res.BackfillComplete && res.Leftover == 0 {
			break
		}
	}
	if fmt.Sprint(tagsOf(st)) != fmt.Sprint(tagsOf(want)) {
		t.Fatalf("resumed %v want %v", tagsOf(st), tagsOf(want))
	}
	for i := range st.Catalogue {
		if st.Catalogue[i].Sequence != want.Catalogue[i].Sequence {
			t.Fatal("sequences differ between resumed and single run")
		}
	}
	if syncs < 2 {
		t.Fatal("budget 4 must need several syncs")
	}
}

func TestCompleteBackfillResolvesNothingBelowMax(t *testing.T) {
	r := newReg(60)
	now := day0.Add(60 * 24 * time.Hour)
	st := apply(State{}, Admit(State{}, r.refs(), r.cfg(now, 7, 3, 1000)))
	r.resolved = nil
	res := Admit(st, r.refs(), r.cfg(now, 7, 3, 1000))
	if len(r.resolved) != 0 || len(res.Admitted) != 0 || !res.BackfillComplete {
		t.Fatalf("a steady-state sync must resolve nothing: %v", r.resolved)
	}
}

func TestBackfillTransientFailureStopsWalkAndHolds(t *testing.T) {
	r := newReg(10)
	r.transient[tag(8)] = true
	now := day0.Add(12 * 24 * time.Hour)
	res := Admit(State{}, r.refs(), r.cfg(now, 0, 3, 100))
	if len(res.Admitted) != 2 || res.BackfillComplete || !res.Waiting || fmt.Sprint(res.Retry) != "[t008]" {
		t.Fatalf("admitted %d complete %v waiting %v retry %v", len(res.Admitted), res.BackfillComplete, res.Waiting, res.Retry)
	}
	st := apply(State{}, res)
	cfg := r.cfg(now, 0, 3, 100)
	cfg.Held = func(tag string) bool { return tag == "t008" }
	n := len(r.resolved)
	res = Admit(st, r.refs(), cfg)
	if len(r.resolved) != n || !res.Waiting {
		t.Fatal("a held tag must not be resolved and must stop the walk")
	}
	r.transient[tag(8)] = false
	res = Admit(st, r.refs(), r.cfg(now, 0, 3, 100))
	if len(res.Admitted) != 8 || !res.BackfillComplete {
		t.Fatalf("after recovery admitted %d", len(res.Admitted))
	}
}

func TestIncrementalTransientIsAdmittedAsStub(t *testing.T) {
	r := newReg(8)
	r.transient[tag(7)] = true
	st := State{BackfillComplete: true, Catalogue: []v1alpha1.Release{{Tag: tag(5), Sequence: 5}}}
	res := Admit(st, r.refs(), r.cfg(day0.Add(20*24*time.Hour), 7, 3, 50))
	if len(res.Admitted) != 3 || !res.Admitted[1].MetadataUnresolved || fmt.Sprint(res.Transient) != "[t007]" {
		t.Fatalf("admitted %+v", res.Admitted)
	}
	if res.Admitted[1].Sequence != 7 {
		t.Fatal("the stub keeps its place in the order")
	}
}

func TestRejectionStoredOnceWithReason(t *testing.T) {
	r := newReg(8)
	r.reject[tag(7)] = "requires has 33 keys"
	st := State{BackfillComplete: true, Catalogue: []v1alpha1.Release{{Tag: tag(5), Sequence: 5}}}
	res := Admit(st, r.refs(), r.cfg(day0.Add(20*24*time.Hour), 7, 3, 50))
	rj := res.Admitted[1]
	if !rj.MetadataUnresolved || rj.Metadata[RejectedKey] != "requires has 33 keys" || len(res.Rejections) != 1 || len(res.Transient) != 0 {
		t.Fatalf("rejected %+v %+v", rj, res.Rejections)
	}
}

func TestTagsInsideTheRangeAreNeverAdmitted(t *testing.T) {
	r := newReg(10)
	st := State{BackfillComplete: true, Catalogue: []v1alpha1.Release{{Tag: tag(3), Sequence: 3}, {Tag: tag(9), Sequence: 4}}}
	res := Admit(st, r.refs(), r.cfg(day0.Add(20*24*time.Hour), 7, 3, 50))
	for _, a := range res.Admitted {
		if a.Tag != tag(10) {
			t.Fatalf("admitted %s", a.Tag)
		}
	}
}

func TestSequencesStrictlyFollowPolicyAfterMixedSyncs(t *testing.T) {
	r := newReg(40)
	now := day0.Add(40 * 24 * time.Hour)
	var st State
	for i := 0; i < 3; i++ {
		st = apply(st, Admit(st, r.refs(), r.cfg(now, 30, 3, 5)))
	}
	// new tags arrive mid-backfill
	for i := 41; i <= 43; i++ {
		r.created[tag(i)] = day0.Add(time.Duration(i) * 24 * time.Hour)
	}
	for i := 0; i < 20; i++ {
		st = apply(st, Admit(st, r.refs(), r.cfg(now.Add(3*24*time.Hour), 30, 3, 5)))
	}
	cat := st.Catalogue
	for i := 1; i < len(cat); i++ {
		if !(cat[i-1].Tag < cat[i].Tag) || cat[i-1].Sequence >= cat[i].Sequence {
			t.Fatalf("order broken at %s/%s seq %d/%d", cat[i-1].Tag, cat[i].Tag, cat[i-1].Sequence, cat[i].Sequence)
		}
	}
	if cat[len(cat)-1].Tag != tag(43) || !st.BackfillComplete {
		t.Fatalf("newest %s complete %v", cat[len(cat)-1].Tag, st.BackfillComplete)
	}
}
