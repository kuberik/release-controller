package slicer

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
)

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func rel(tag string, mins int) v1alpha1.Release {
	ts := metav1.NewTime(t0.Add(time.Duration(mins) * time.Minute))
	return v1alpha1.Release{Tag: tag, Created: ts, FirstSeen: ts}
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

func TestFromScratchSortsAndChunks(t *testing.T) {
	p := Compute(Input{New: []v1alpha1.Release{rel("e", 5), rel("a", 1), rel("d", 4), rel("b", 2), rel("c", 3)}}, cfg(2))
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

func TestLateReleaseWithOlderCreatedSorts(t *testing.T) {
	c := cfg(5)
	s := Compute(Input{New: []v1alpha1.Release{rel("b", 2), rel("d", 4)}}, c).Final
	p := Compute(Input{Existing: s, New: []v1alpha1.Release{rel("a", 1), rel("c", 3)}}, c)
	eq(t, tags(p.Updates[0]), "a", "b", "c", "d")
	if !p.Updates[0].OldestCreated.Time.Equal(t0.Add(time.Minute)) {
		t.Fatal("oldest bound")
	}
}

func TestTieBrokenByTag(t *testing.T) {
	p := Compute(Input{New: []v1alpha1.Release{rel("z", 1), rel("m", 1), rel("a", 1)}}, cfg(10))
	eq(t, tags(p.Creates[0]), "a", "m", "z")
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
	d := "sha256:x"
	fixed.Digest = &d
	fixed.FirstSeen = metav1.NewTime(t0.Add(99 * time.Hour)) // must be ignored
	p := Compute(Input{Existing: s, Repairs: []v1alpha1.Release{fixed}}, c)
	if len(p.Updates) != 1 || p.Updates[0].Name != "src-00000" || len(p.Creates) != 0 {
		t.Fatalf("plan %+v", p)
	}
	got := p.Updates[0].Releases[0]
	if got.MetadataUnresolved || got.Digest == nil || !got.FirstSeen.Equal(&metav1.Time{Time: t0.Add(time.Minute)}) || !p.Updates[0].Sealed {
		t.Fatalf("repair %+v", got)
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

func TestRetentionStopsAtFirstUnexpired(t *testing.T) {
	c := cfg(2)
	c.RetentionDays = 7
	old, fresh := rel("old", 1), rel("fresh", 1)
	fresh.Created = metav1.NewTime(c.Now)
	s := Compute(Input{New: []v1alpha1.Release{old, fresh, rel("x", 60*24*30+5)}}, cfg(2)).Final
	// slice0 = old + x? sort: old(1m), fresh? fresh created=now > x. Just assert partial slice kept.
	p := Compute(Input{Existing: s}, c)
	for _, d := range p.Deletes {
		for _, sl := range s {
			if sl.Name == d {
				for _, r := range sl.Releases {
					if r.Tag == "fresh" {
						t.Fatal("deleted a slice with an unexpired release")
					}
				}
			}
		}
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
	c.NextIndex = 0                                                      // status lost
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

func lessTag(a, b v1alpha1.Release) bool { return a.Tag < b.Tag }

func TestAdmitMonotonic(t *testing.T) {
	cat := []v1alpha1.Release{rel("v5", 5)}
	adm, rej := Admit(cat, []v1alpha1.Release{rel("v3", 6), rel("v7", 7), rel("v6", 8), rel("v9", 9)}, lessTag, false)
	// sorted by created: v3,v7,v6,v9 -> v3 rejected (<v5), v7 ok, v6 rejected (<v7), v9 ok
	eq(t, []string{adm[0].Tag, adm[1].Tag}, "v7", "v9")
	if len(rej) != 2 {
		t.Fatal("rejected")
	}
}

func TestAdmitFromScratchRunningMax(t *testing.T) {
	adm, _ := Admit(nil, []v1alpha1.Release{rel("v2", 2), rel("v1", 3), rel("v3", 4)}, lessTag, false)
	eq(t, []string{adm[0].Tag, adm[1].Tag}, "v2", "v3")
}

func TestAdmitAll(t *testing.T) {
	adm, _ := Admit([]v1alpha1.Release{rel("v9", 9)}, []v1alpha1.Release{rel("v1", 1)}, lessTag, true)
	if len(adm) != 1 {
		t.Fatal("admit all")
	}
}
