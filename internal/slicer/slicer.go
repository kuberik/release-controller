// Package slicer computes ReleaseSlice writes. It is pure: no client, no
// clock, no I/O. Input is the slices that exist plus what the sync found;
// output is the minimal set of creates, updates and deletes (design §3).
//
// Rules in one place:
//   - releases keep the slice they are in; new ones are appended to the open
//     slice (the one with sealed=false) and every slice is sorted by
//     (created, tag);
//   - a slice is sealed in the same write that fills it, at MaxPerSlice
//     releases or when its encoded size reaches MaxBytes; overflow goes into a
//     new slice named <source>-<5-digit index>;
//   - repairs replace an unresolved release in place, even in a sealed slice
//     (the only write a sealed slice takes);
//   - retention deletes the oldest sealed slices while every release in them
//     is expired and the remaining count stays >= MinCount; the open slice is
//     never deleted;
//   - a tag present in several slices keeps the older slice's copy.
package slicer

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
)

// Config carries everything the plan depends on besides the slices.
type Config struct {
	Namespace string
	// Source is the ReleaseSource name; slices are named <Source>-<index>.
	Source string
	// Owner is set on created slices (controller=true).
	Owner metav1.OwnerReference
	// Annotations are set on created slices.
	Annotations map[string]string
	// NextIndex is the index of the next slice to create. It is raised above
	// any existing slice's index so names never collide.
	NextIndex int32
	Now       time.Time
	// RetentionDays 0 disables age expiry.
	RetentionDays int32
	MinCount      int
	// MaxPerSlice seals at this many releases (N).
	MaxPerSlice int
	// MaxBytes seals once the encoded slice object reaches this size; 0 disables.
	MaxBytes int64
}

// Input is what the sync found.
type Input struct {
	Existing []v1alpha1.ReleaseSlice
	// New are admitted releases not in the catalogue. Tags already present are ignored.
	New []v1alpha1.Release
	// Repairs are resolved releases for catalogue entries that were unresolved.
	// FirstSeen of the stored entry is kept.
	Repairs []v1alpha1.Release
}

// Plan is the minimal write set. Updates carry the full new object.
type Plan struct {
	Creates []v1alpha1.ReleaseSlice
	Updates []v1alpha1.ReleaseSlice
	Deletes []string
	// Final is the state after the plan, oldest to newest, deleted slices excluded.
	Final     []v1alpha1.ReleaseSlice
	NextIndex int32
}

// Empty reports whether the plan writes nothing.
func (p Plan) Empty() bool {
	return len(p.Creates) == 0 && len(p.Updates) == 0 && len(p.Deletes) == 0
}

// Less orders releases by (created, tag).
func Less(a, b v1alpha1.Release) bool {
	if !a.Created.Equal(&b.Created) {
		return a.Created.Before(&b.Created)
	}
	return a.Tag < b.Tag
}

// Merge returns the catalogue of a set of slices: every release once, sorted
// by (created, tag). If a tag appears in several slices the copy in the older
// slice (lower name, since names are zero-padded indices) wins. Readers
// (rollout-controller) use exactly this.
func Merge(slices []v1alpha1.ReleaseSlice) []v1alpha1.Release {
	ordered := sortedByName(slices)
	seen := map[string]struct{}{}
	var out []v1alpha1.Release
	for _, s := range ordered {
		for _, r := range s.Releases {
			if _, dup := seen[r.Tag]; dup {
				continue
			}
			seen[r.Tag] = struct{}{}
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return Less(out[i], out[j]) })
	return out
}

// EncodedSize is the JSON size of a slice object, the quantity MaxBytes bounds.
func EncodedSize(s *v1alpha1.ReleaseSlice) int {
	b, _ := json.Marshal(s)
	return len(b)
}

// SliceName is the deterministic slice name.
func SliceName(source string, index int32) string {
	return fmt.Sprintf("%s-%05d", source, index)
}

// Index parses the index out of a slice name of the given source.
func Index(source, name string) (int32, bool) {
	rest, ok := strings.CutPrefix(name, source+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(rest, 10, 32)
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

func sortedByName(in []v1alpha1.ReleaseSlice) []v1alpha1.ReleaseSlice {
	out := make([]v1alpha1.ReleaseSlice, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func norm(r v1alpha1.Release) v1alpha1.Release {
	r = *r.DeepCopy()
	r.Created = metav1.NewTime(r.Created.UTC().Truncate(time.Second))
	r.FirstSeen = metav1.NewTime(r.FirstSeen.UTC().Truncate(time.Second))
	return r
}

func sortReleases(rs []v1alpha1.Release) {
	sort.SliceStable(rs, func(i, j int) bool { return Less(rs[i], rs[j]) })
}

func setBounds(s *v1alpha1.ReleaseSlice) {
	if len(s.Releases) == 0 {
		s.OldestCreated, s.NewestCreated = nil, nil
		return
	}
	o := s.Releases[0].Created
	n := s.Releases[len(s.Releases)-1].Created
	s.OldestCreated, s.NewestCreated = &o, &n
}

// Compute returns the minimal plan for the input.
func Compute(in Input, cfg Config) Plan {
	if cfg.MaxPerSlice <= 0 {
		cfg.MaxPerSlice = 100
	}
	orig := sortedByName(in.Existing)
	slices := make([]v1alpha1.ReleaseSlice, len(orig))
	next := cfg.NextIndex
	for i := range orig {
		slices[i] = *orig[i].DeepCopy()
		if idx, ok := Index(cfg.Source, slices[i].Name); ok && idx >= next {
			next = idx + 1
		}
	}

	// Duplicate tags: the older slice keeps its copy.
	owner := map[string]int{}
	for i := range slices {
		kept := slices[i].Releases[:0:0]
		for _, r := range slices[i].Releases {
			if _, dup := owner[r.Tag]; dup {
				continue
			}
			owner[r.Tag] = i
			kept = append(kept, r)
		}
		if len(kept) != len(slices[i].Releases) {
			slices[i].Releases = kept
			setBounds(&slices[i])
		}
	}

	// Repairs, in place.
	for _, rep := range in.Repairs {
		i, ok := owner[rep.Tag]
		if !ok {
			continue
		}
		rep = norm(rep)
		for j := range slices[i].Releases {
			old := slices[i].Releases[j]
			if old.Tag != rep.Tag {
				continue
			}
			rep.FirstSeen = old.FirstSeen
			if rep.Created.IsZero() {
				rep.Created, rep.CreatedObserved = old.Created, old.CreatedObserved
			}
			slices[i].Releases[j] = rep
		}
		sortReleases(slices[i].Releases)
		setBounds(&slices[i])
	}

	// Append.
	var fresh []v1alpha1.Release
	seenNew := map[string]struct{}{}
	for _, r := range in.New {
		if _, ok := owner[r.Tag]; ok {
			continue
		}
		if _, ok := seenNew[r.Tag]; ok {
			continue
		}
		seenNew[r.Tag] = struct{}{}
		fresh = append(fresh, norm(r))
	}
	created := map[string]bool{}
	if len(fresh) > 0 {
		open := -1
		for i := range slices {
			if !slices[i].Sealed {
				open = i
			}
		}
		var pool []v1alpha1.Release
		if open >= 0 {
			pool = append(pool, slices[open].Releases...)
		}
		pool = append(pool, fresh...)
		sortReleases(pool)

		newSlice := func() v1alpha1.ReleaseSlice {
			name := SliceName(cfg.Source, next)
			next++
			created[name] = true
			o := cfg.Owner
			return v1alpha1.ReleaseSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name: name, Namespace: cfg.Namespace,
					Labels:          map[string]string{v1alpha1.ReleaseSourceLabel: cfg.Source},
					Annotations:     cfg.Annotations,
					OwnerReferences: []metav1.OwnerReference{o},
				},
				Source: cfg.Source,
			}
		}
		for len(pool) > 0 {
			var t v1alpha1.ReleaseSlice
			idx := open
			if open >= 0 {
				t = slices[open]
				open = -1 // later overflow goes to new slices
			} else {
				t = newSlice()
				slices = append(slices, t)
				idx = len(slices) - 1
			}
			t.Releases = nil
			size := int64(EncodedSize(&t))
			for len(pool) > 0 {
				r := pool[0]
				pool = pool[1:]
				t.Releases = append(t.Releases, r)
				b, _ := json.Marshal(r)
				size += int64(len(b)) + 1
				if len(t.Releases) >= cfg.MaxPerSlice || (cfg.MaxBytes > 0 && size >= cfg.MaxBytes) {
					t.Sealed = true
					break
				}
			}
			setBounds(&t)
			slices[idx] = t
		}
	}

	// Retention: oldest sealed slices only.
	total := 0
	for _, s := range slices {
		total += len(s.Releases)
	}
	deleted := map[string]bool{}
	if cfg.RetentionDays > 0 {
		cutoff := cfg.Now.Add(-time.Duration(cfg.RetentionDays) * 24 * time.Hour)
		for _, s := range slices {
			if !s.Sealed || total-len(s.Releases) < cfg.MinCount {
				break
			}
			expired := true
			for _, r := range s.Releases {
				if !r.Created.Time.Before(cutoff) {
					expired = false
					break
				}
			}
			if !expired {
				break
			}
			deleted[s.Name] = true
			total -= len(s.Releases)
		}
	}

	origByName := map[string]v1alpha1.ReleaseSlice{}
	for _, s := range orig {
		origByName[s.Name] = s
	}
	plan := Plan{NextIndex: next}
	for _, s := range slices {
		switch {
		case deleted[s.Name]:
			if !created[s.Name] {
				plan.Deletes = append(plan.Deletes, s.Name)
			}
			continue
		case created[s.Name]:
			plan.Creates = append(plan.Creates, s)
		default:
			if !apiequality.Semantic.DeepEqual(origByName[s.Name], s) {
				plan.Updates = append(plan.Updates, s)
			}
		}
		plan.Final = append(plan.Final, s)
	}
	return plan
}

// Admit applies monotonic admission. Candidates (resolved, created/firstSeen
// filled) are taken in (created, tag) order and admitted only if the running
// maximum under less (seeded with the catalogue's maximum) is less than them;
// each admitted candidate becomes the new maximum. With an empty catalogue
// this is the from-scratch running-maximum rule. admitAll bypasses the rule
// (a source with no policy).
func Admit(catalogue, candidates []v1alpha1.Release, less func(a, b v1alpha1.Release) bool, admitAll bool) (admitted, rejected []v1alpha1.Release) {
	sorted := append([]v1alpha1.Release(nil), candidates...)
	sortReleases(sorted)
	if admitAll || less == nil {
		return sorted, nil
	}
	var max *v1alpha1.Release
	for i := range catalogue {
		if max == nil || less(*max, catalogue[i]) {
			max = &catalogue[i]
		}
	}
	for _, c := range sorted {
		if max != nil && !less(*max, c) {
			rejected = append(rejected, c)
			continue
		}
		admitted = append(admitted, c)
		c := c
		max = &c
	}
	return admitted, rejected
}

// DropExpired removes candidates that are already expired by age (created
// before now - days) so retention and admission do not fight over a tag the
// source still lists. The newest expired candidates are kept only as far as
// needed to bring the catalogue (count releases already) up to minCount.
// days == 0 disables the filter. The result keeps (created, tag) order.
func DropExpired(count int, candidates []v1alpha1.Release, now time.Time, days int32, minCount int) (kept, dropped []v1alpha1.Release) {
	if days <= 0 {
		return candidates, nil
	}
	cutoff := now.Add(-time.Duration(days) * 24 * time.Hour)
	fresh := 0
	for _, c := range candidates {
		if !c.Created.Time.Before(cutoff) {
			fresh++
		}
	}
	need := minCount - count - fresh // expired candidates still wanted, newest first
	keep := map[string]bool{}
	for i := len(candidates) - 1; i >= 0; i-- {
		c := candidates[i]
		if !c.Created.Time.Before(cutoff) {
			keep[c.Tag] = true
		} else if need > 0 {
			keep[c.Tag] = true
			need--
		}
	}
	for _, c := range candidates {
		if keep[c.Tag] {
			kept = append(kept, c)
		} else {
			dropped = append(dropped, c)
		}
	}
	return kept, dropped
}
