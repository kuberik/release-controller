package slicer

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
)

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

// rel is a release with sequence == mins and created mins minutes after t0.
func rel(tag string, mins int) v1alpha1.Release {
	ts := metav1.NewTime(t0.Add(time.Duration(mins) * time.Minute))
	return v1alpha1.Release{Tag: tag, Sequence: int64(mins), Created: ts, FirstSeen: ts}
}

func cfg(n int) Config {
	return Config{Namespace: "ns", Source: "src", Now: t0.Add(30 * 24 * time.Hour), MaxPerSlice: n}
}

func tags(s v1alpha1.ReleaseSlice) (out []string) {
	for _, r := range s.Releases {
		out = append(out, r.Tag)
	}
	return
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFromScratchChunksInAdmissionOrder(t *testing.T) {
	p := Compute(Input{New: []v1alpha1.Release{rel("a", 1), rel("b", 2), rel("c", 3), rel("d", 4), rel("e", 5)}}, cfg(2))
	if len(p.Creates) != 3 || len(p.Updates) != 0 || len(p.Deletes) != 0 {
		t.Fatalf("plan %+v", p)
	}
	eq(t, tags(p.Creates[0]), "a", "b")
	eq(t, tags(p.Creates[1]), "c", "d")
	eq(t, tags(p.Creates[2]), "e")
	if !p.Creates[0].Sealed || !p.Creates[1].Sealed || p.Creates[2].Sealed {
		t.Fatal("only the last chunk is open")
	}
	if p.Creates[0].Name != "src-00000" || p.Creates[2].Name != "src-00002" || p.NextIndex != 3 {
		t.Fatalf("names %s %s next %d", p.Creates[0].Name, p.Creates[2].Name, p.NextIndex)
	}
	if p.Creates[0].OldestCreated == nil || !p.Creates[0].NewestCreated.Time.Equal(t0.Add(2*time.Minute)) {
		t.Fatal("bounds")
	}
}

func TestAppendIntoOpenOnlyTouchesOpen(t *testing.T) {
	c := cfg(3)
	s := Compute(Input{New: []v1alpha1.Release{rel("a", 1), rel("b", 2), rel("c", 3), rel("d", 4)}}, c).Final
	c.NextIndex = 2
	p := Compute(Input{Existing: s, New: []v1alpha1.Release{rel("e", 5)}}, c)
	if len(p.Updates) != 1 || p.Updates[0].Name != "src-00001" || len(p.Creates) != 0 {
		t.Fatalf("plan %+v", p)
	}
	eq(t, tags(p.Updates[0]), "d", "e")
}

func TestSealAtNSameWrite(t *testing.T) {
	c := cfg(3)
	s := Compute(Input{New: []v1alpha1.Release{rel("a", 1)}}, c).Final
	p := Compute(Input{Existing: s, New: []v1alpha1.Release{rel("b", 2), rel("c", 3), rel("d", 4)}}, c)
	if len(p.Updates) != 1 || !p.Updates[0].Sealed || len(p.Creates) != 1 {
		t.Fatalf("plan %+v", p)
	}
	eq(t, tags(p.Updates[0]), "a", "b", "c")
	eq(t, tags(p.Creates[0]), "d")
	if p.Creates[0].Sealed {
		t.Fatal("new slice open")
	}
}

func TestSealExactlyFullLeavesNoOpen(t *testing.T) {
	p := Compute(Input{New: []v1alpha1.Release{rel("a", 1), rel("b", 2)}}, cfg(2))
	if len(p.Creates) != 1 || !p.Creates[0].Sealed {
		t.Fatal("sealed on fill")
	}
	p2 := Compute(Input{Existing: p.Final, New: []v1alpha1.Release{rel("c", 3)}}, Config{Source: "src", Namespace: "ns", MaxPerSlice: 2, Now: t0})
	if len(p2.Creates) != 1 || len(p2.Updates) != 0 || p2.Creates[0].Name != "src-00001" {
		t.Fatalf("plan %+v", p2)
	}
}

func TestSealOnByteValve(t *testing.T) {
	c := cfg(1000)
	one := rel("a", 1)
	empty := &v1alpha1.ReleaseSlice{ObjectMeta: metav1.ObjectMeta{Name: "src-00000", Namespace: "ns"}, Source: "src"}
	c.MaxBytes = int64(EncodedSize(empty) + 3*(EncodedSize2(one)+1))
	var in []v1alpha1.Release
	for i := 0; i < 10; i++ {
		in = append(in, rel(fmt.Sprintf("a%d", i), i))
	}
	p := Compute(Input{New: in}, c)
	if len(p.Creates) < 2 || !p.Creates[0].Sealed {
		t.Fatalf("expected byte seal, got %d slices", len(p.Creates))
	}
	if len(p.Creates[0].Releases) > 4 {
		t.Fatalf("slice 0 has %d releases", len(p.Creates[0].Releases))
	}
	if EncodedSize(&p.Creates[0]) < int(c.MaxBytes) {
		t.Fatal("sealed below valve")
	}
}

func EncodedSize2(r v1alpha1.Release) int {
	return EncodedSize(&v1alpha1.ReleaseSlice{Releases: []v1alpha1.Release{r}}) - EncodedSize(&v1alpha1.ReleaseSlice{Releases: []v1alpha1.Release{}})
}


// Existing releases never move: a backfill batch (lower sequences, descending
// admission order) fills the open slice next to its neighbours and overflows
// into new slices; every slice is sorted by sequence.
func TestBackfillFillsOpenSliceAndOverflows(t *testing.T) {
	c := cfg(3)
	s := Compute(Input{New: []v1alpha1.Release{rel("k10", 10), rel("k11", 11)}}, c).Final // one open slice
	c.NextIndex = 1
	p := Compute(Input{Existing: s, New: []v1alpha1.Release{rel("k9", 9), rel("k8", 8), rel("k7", 7), rel("k6", 6)}}, c)
	if len(p.Updates) != 1 || len(p.Creates) != 1 {
		t.Fatalf("plan %+v", p)
	}
	eq(t, tags(p.Updates[0]), "k9", "k10", "k11") // k10,k11 stayed, k9 joined, sorted by sequence
	if !p.Updates[0].Sealed {
		t.Fatal("sealed in the same write")
	}
	eq(t, tags(p.Creates[0]), "k6", "k7", "k8")
	if !p.Creates[0].Sealed {
		t.Fatal("full overflow slice is sealed")
	}
}

func TestMergeOrdersBySequenceNotCreated(t *testing.T) {
	late := rel("retag", 1)
	late.Sequence = 99 // newest by policy although created long ago
	s := Compute(Input{New: []v1alpha1.Release{rel("a", 5), late}}, cfg(5)).Final
	m := Merge(s)
	if m[len(m)-1].Tag != "retag" {
		t.Fatalf("got %v", m)
	}
	eq(t, tags(s[0]), "a", "retag")
}

func TestUnchangedIsEmptyPlan(t *testing.T) {
	c := cfg(2)
	s := Compute(Input{New: []v1alpha1.Release{rel("a", 1), rel("b", 2), rel("c", 3)}}, c).Final
	p := Compute(Input{Existing: s, New: []v1alpha1.Release{rel("a", 1), rel("c", 3)}}, c)
	if !p.Empty() {
		t.Fatalf("expected empty plan, got %+v", p)
	}
}

func TestRepairInSealedSlice(t *testing.T) {
	c := cfg(2)
	u := rel("a", 1)
	u.MetadataUnresolved = true
	s := Compute(Input{New: []v1alpha1.Release{u, rel("b", 2), rel("c", 3)}}, c).Final
	fixed := rel("a", 1)
	fixed.Sequence = 500 // must be ignored
	d := "sha256:x"
	fixed.Digest = &d
	fixed.FirstSeen = metav1.NewTime(t0.Add(99 * time.Hour)) // must be ignored
	p := Compute(Input{Existing: s, Repairs: []v1alpha1.Release{fixed}}, c)
	if len(p.Updates) != 1 || p.Updates[0].Name != "src-00000" || len(p.Creates) != 0 {
		t.Fatalf("plan %+v", p)
	}
	got := p.Updates[0].Releases[0]
	if got.MetadataUnresolved || got.Digest == nil || got.Sequence != 1 || !got.FirstSeen.Equal(&metav1.Time{Time: t0.Add(time.Minute)}) || !p.Updates[0].Sealed {
		t.Fatalf("repair %+v", got)
	}
}

// QA probe P6: a repair that would push a slice over the hard limit marks the
// release rejected instead and does not grow the slice.
func TestRepairBeyondHardLimitIsRejectedNotGrown(t *testing.T) {
	var news []v1alpha1.Release
	for i := 0; i < 100; i++ {
		r := rel(fmt.Sprintf("main-%010d-%040d", i, i), i)
		r.MetadataUnresolved, r.CreatedObserved = true, true
		news = append(news, r)
	}
	c := Config{Namespace: "ns", Source: "s", Now: t0, MaxPerSlice: 100, MaxBytes: 256 * 1024}
	p := Compute(Input{New: news}, c)
	before := EncodedSize(&p.Final[0])
	var reps []v1alpha1.Release
	for _, r := range news {
		r2 := r
		r2.MetadataUnresolved, r2.CreatedObserved = false, false
		d := "sha256:" + strings.Repeat("a", 64)
		v, rv := strings.Repeat("v", 128), strings.Repeat("r", 128)
		r2.Digest, r2.Version, r2.Revision = &d, &v, &rv
		r2.Requires = map[string]string{}
		for k := 0; k < 32; k++ {
			r2.Requires[fmt.Sprintf("%02d%s", k, strings.Repeat("k", 62))] = strings.Repeat("x", 256)
		}
		reps = append(reps, r2)
	}
	c.HardMaxBytes = 1 << 20
	p2 := Compute(Input{Existing: p.Final, Repairs: reps}, c)
	if got := EncodedSize(&p2.Final[0]); int64(got) > c.HardMaxBytes {
		t.Fatalf("slice grew to %d over the %d limit (was %d)", got, c.HardMaxBytes, before)
	}
	if len(p2.RepairsRejected) == 0 || len(p2.RepairsRejected) == len(reps) {
		t.Fatalf("expected some, not all repairs rejected: %d", len(p2.RepairsRejected))
	}
	rejected := 0
	for _, r := range p2.Final[0].Releases {
		if _, ok := r.Metadata[RejectedKey]; ok {
			rejected++
			if !r.MetadataUnresolved {
				t.Fatal("a rejected release stays unresolved")
			}
		}
	}
	if rejected != len(p2.RepairsRejected) {
		t.Fatalf("rejected %d vs reported %d", rejected, len(p2.RepairsRejected))
	}
}

func TestRetention(t *testing.T) {
	c := cfg(2)
	c.RetentionDays = 7
	var in []v1alpha1.Release
	for i := 0; i < 7; i++ { // created i minutes after t0, now = t0+30d: all expired
		in = append(in, rel(fmt.Sprintf("r%d", i), i))
	}
	s := Compute(Input{New: in}, cfg(2)).Final // 4 slices: 2,2,2,1(open)
	// minCount 3: delete slices 0 and 1 (7-2=5>=3, 5-2=3>=3), slice 2 would leave 1 < 3.
	c.MinCount = 3
	p := Compute(Input{Existing: s}, c)
	eq(t, p.Deletes, "src-00000", "src-00001")
	if len(p.Final) != 2 {
		t.Fatal("final")
	}
	// minCount 0: all sealed go, open stays.
	c.MinCount = 0
	p = Compute(Input{Existing: s}, c)
	eq(t, p.Deletes, "src-00000", "src-00001", "src-00002")
	if len(p.Final) != 1 || p.Final[0].Sealed {
		t.Fatal("open slice must survive")
	}
	// days 0: nothing expires.
	c.RetentionDays = 0
	if p = Compute(Input{Existing: s}, c); !p.Empty() {
		t.Fatal("days=0 must not expire")
	}
}

// "Oldest" is by minimum sequence, not by name: after a backfill the names
// run opposite to the sequences.
func TestRetentionOldestByMinSequenceNotName(t *testing.T) {
	c := cfg(2)
	c.RetentionDays = 7
	c.MinCount = 0
	// slice 00000 holds the NEWEST (seq 9,10, fresh), 00001 the older (7,8), 00002 the oldest (5,6).
	fresh := func(tag string, seq int) v1alpha1.Release {
		r := rel(tag, seq)
		r.Created = metav1.NewTime(c.Now)
		return r
	}
	s0 := Compute(Input{New: []v1alpha1.Release{fresh("n10", 10), fresh("n9", 9)}}, cfg(2)).Final
	c1 := cfg(2)
	c1.NextIndex = 1
	s1 := Compute(Input{Existing: s0, New: []v1alpha1.Release{rel("o8", 8), rel("o7", 7), rel("p6", 6), rel("p5", 5), rel("p4", 4)}}, c1).Final
	p := Compute(Input{Existing: s1}, c)
	// expired: 00001 (8,7) and 00002 (6,5); 00000 is fresh and sealed; the open 00003 (4) stays.
	eq(t, p.Deletes, "src-00001", "src-00002")
}

func TestRetentionStopsAtFirstUnexpired(t *testing.T) {
	c := cfg(2)
	c.RetentionDays = 7
	old, fresh := rel("old", 1), rel("fresh", 2)
	fresh.Created = metav1.NewTime(c.Now)
	s := Compute(Input{New: []v1alpha1.Release{old, fresh, rel("x", 3)}}, cfg(2)).Final
	p := Compute(Input{Existing: s}, c)
	if len(p.Deletes) != 0 {
		t.Fatalf("deleted a slice with an unexpired release: %v", p.Deletes)
	}
}

func TestDuplicateKeepsOlder(t *testing.T) {
	c := cfg(5)
	older := Compute(Input{New: []v1alpha1.Release{rel("a", 1), rel("b", 2)}}, c).Final[0]
	newer := older.DeepCopy()
	newer.Name = "src-00001"
	newer.Releases = []v1alpha1.Release{rel("b", 2), rel("c", 3)}
	c.NextIndex = 2
	p := Compute(Input{Existing: []v1alpha1.ReleaseSlice{*newer, older}}, c)
	if len(p.Updates) != 1 || p.Updates[0].Name != "src-00001" {
		t.Fatalf("plan %+v", p)
	}
	eq(t, tags(p.Updates[0]), "c")
	m := Merge([]v1alpha1.ReleaseSlice{*newer, older})
	if len(m) != 3 {
		t.Fatal("merge dedupe")
	}
}

func TestNextIndexRaisedAboveExisting(t *testing.T) {
	c := cfg(1)
	s := Compute(Input{New: []v1alpha1.Release{rel("a", 1)}}, c).Final // sealed 00000
	c.NextIndex = 0                                                    // status lost
	p := Compute(Input{Existing: s, New: []v1alpha1.Release{rel("b", 2)}}, c)
	if p.Creates[0].Name != "src-00001" {
		t.Fatal(p.Creates[0].Name)
	}
}

func TestSubSecondNoSpuriousUpdate(t *testing.T) {
	c := cfg(5)
	r := rel("a", 1)
	r.Created = metav1.NewTime(r.Created.Add(123456789 * time.Nanosecond))
	s := Compute(Input{New: []v1alpha1.Release{r}}, c).Final
	if p := Compute(Input{Existing: s, New: []v1alpha1.Release{r}}, c); !p.Empty() {
		t.Fatal("spurious")
	}
}
