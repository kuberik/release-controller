// Package slicer computes ReleaseSlice writes. It is pure: no client, no
// clock, no I/O. Input is the slices that exist plus what the sync admitted;
// output is the minimal set of creates, updates and deletes (design §3, §3a).
//
// Rules in one place:
//   - the catalogue is ordered by Sequence (admission order = policy order),
//     never by created; every slice is sorted by sequence;
//   - releases never move between slices; new releases (given in admission
//     order: incremental ascending, then backfill descending) fill the open
//     slice (sealed=false) and the overflow goes into new slices named
//     <source>-<5-digit index>;
//   - a slice is sealed in the same write that fills it, at MaxPerSlice
//     releases or when its encoded size reaches MaxBytes;
//   - repairs replace an unresolved release in place, even in a sealed slice;
//     a repair that would push the slice over HardMaxBytes (1 MiB) instead
//     marks the release rejected, without growing the slice;
//   - retention deletes the oldest sealed slices (by min sequence) while every
//     release in them is expired and the remaining count stays >= MinCount;
//     the open slice is never deleted;
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
	// HardMaxBytes is the encoded size a repair may never exceed (default 1 MiB,
	// well under etcd's 1.5 MiB request limit).
	HardMaxBytes int64
}

// DefaultHardMaxBytes is the repair size limit.
const DefaultHardMaxBytes int64 = 1 << 20

// RejectedKey is the Release.Metadata key that marks a permanent rejection
// (the reason is the value). A rejected release is stored once with
// metadataUnresolved=true and never retried.
const RejectedKey = "kuberik.com/rejected"

// Input is what the sync found.
type Input struct {
	Existing []v1alpha1.ReleaseSlice
	// New are admitted releases not in the catalogue, with Sequence set, in
	// admission order. Tags already present are ignored.
	New []v1alpha1.Release
	// Repairs are resolved releases for catalogue entries that were unresolved.
	// Sequence and FirstSeen of the stored entry are kept. A repair carrying
	// RejectedKey in Metadata marks the entry rejected.
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
	// RepairsRejected are tags whose repair was turned into a rejection
	// because it would have grown a slice over HardMaxBytes.
	RepairsRejected []string
}

// Empty reports whether the plan writes nothing.
func (p Plan) Empty() bool {
	return len(p.Creates) == 0 && len(p.Updates) == 0 && len(p.Deletes) == 0
}

// Less orders releases by (sequence, tag).
func Less(a, b v1alpha1.Release) bool {
	if a.Sequence != b.Sequence {
		return a.Sequence < b.Sequence
	}
	return a.Tag < b.Tag
}

// Merge returns the catalogue of a set of slices: every release once, sorted
// by (sequence, tag). If a tag appears in several slices the copy in the older
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
	o, n := s.Releases[0].Created, s.Releases[0].Created
	for _, r := range s.Releases[1:] {
		if r.Created.Before(&o) {
			o = r.Created
		}
		if n.Before(&r.Created) {
			n = r.Created
		}
	}
	s.OldestCreated, s.NewestCreated = &o, &n
}

func minSeq(s v1alpha1.ReleaseSlice) int64 {
	if len(s.Releases) == 0 {
		return 0
	}
	m := s.Releases[0].Sequence
	for _, r := range s.Releases[1:] {
		if r.Sequence < m {
			m = r.Sequence
		}
	}
	return m
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

	if cfg.HardMaxBytes <= 0 {
		cfg.HardMaxBytes = DefaultHardMaxBytes
	}
	var repairRejected []string

	// Repairs, in place. Sequence and FirstSeen stay; the slice may not grow
	// over the hard limit: then the release is marked rejected instead.
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
			rep.FirstSeen, rep.Sequence = old.FirstSeen, old.Sequence
			if rep.Created.IsZero() {
				rep.Created, rep.CreatedObserved = old.Created, old.CreatedObserved
			}
			grown := slices[i]
			grown.Releases = append([]v1alpha1.Release(nil), slices[i].Releases...)
			grown.Releases[j] = rep
			if _, rejected := rep.Metadata[RejectedKey]; !rejected && int64(EncodedSize(&grown)) > cfg.HardMaxBytes {
				rj := old
				rj.MetadataUnresolved = true
				rj.Metadata = map[string]string{RejectedKey: "resolved metadata would grow the slice over the size limit"}
				rep = rj
				repairRejected = append(repairRejected, rep.Tag)
			}
			slices[i].Releases[j] = rep
		}
		sortReleases(slices[i].Releases)
		setBounds(&slices[i])
	}

	// Append in admission order: the open slice first, then new slices.
	fresh := make([]v1alpha1.Release, 0, len(in.New))
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
		full := func(t *v1alpha1.ReleaseSlice, size int64) bool {
			return len(t.Releases) >= cfg.MaxPerSlice || (cfg.MaxBytes > 0 && size >= cfg.MaxBytes)
		}
		touched := map[int]bool{}
		cur := open // index into slices of the slice taking releases; -1 none
		var size int64
		if cur >= 0 {
			size = int64(EncodedSize(&slices[cur]))
			if full(&slices[cur], size) {
				slices[cur].Sealed = true
				cur = -1
			}
		}
		for _, r := range fresh {
			if cur < 0 {
				slices = append(slices, newSlice())
				cur = len(slices) - 1
				size = int64(EncodedSize(&slices[cur]))
			}
			touched[cur] = true
			t := &slices[cur]
			t.Releases = append(t.Releases, r)
			b, _ := json.Marshal(r)
			size += int64(len(b)) + 1
			if full(t, size) {
				t.Sealed = true
				cur = -1
			}
		}
		for i := range slices {
			if touched[i] {
				sortReleases(slices[i].Releases)
				setBounds(&slices[i])
			}
		}
	}

	// Retention: oldest sealed slices only (oldest = lowest min sequence).
	total := 0
	for _, s := range slices {
		total += len(s.Releases)
	}
	deleted := map[string]bool{}
	var maxSeq int64
	for i, s := range slices {
		for j, r := range s.Releases {
			if (i == 0 && j == 0) || r.Sequence > maxSeq {
				maxSeq = r.Sequence
			}
		}
	}
	holdsNewest := func(s v1alpha1.ReleaseSlice) bool {
		for _, r := range s.Releases {
			if r.Sequence == maxSeq {
				return true
			}
		}
		return false
	}
	if cfg.RetentionDays > 0 {
		cutoff := cfg.Now.Add(-time.Duration(cfg.RetentionDays) * 24 * time.Hour)
		bySeq := make([]v1alpha1.ReleaseSlice, len(slices))
		copy(bySeq, slices)
		sort.SliceStable(bySeq, func(i, j int) bool { return minSeq(bySeq[i]) < minSeq(bySeq[j]) })
		for _, s := range bySeq {
			// The open slice is never deleted. If it holds the lowest sequences
			// (a backfill filled it) it also stops retention: deleting newer
			// sealed slices past it would leave a hole below kept releases.
			// The slice holding the newest release stays too: it is the anchor
			// of incremental admission (a catalogue maximum that vanished
			// would re-admit everything above the next one).
			if !s.Sealed || holdsNewest(s) || total-len(s.Releases) < cfg.MinCount {
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
	plan := Plan{NextIndex: next, RepairsRejected: repairRejected}
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

// CheckSequences reports an error if two different tags in the catalogue
// share a sequence. A plan that would write such a catalogue is refused: the
// reader's order would depend on the tag tie-break, and a sealed slice cannot
// be renumbered afterwards.
func CheckSequences(final []v1alpha1.ReleaseSlice) error {
	bySeq := map[int64]string{}
	for _, r := range Merge(final) {
		if other, ok := bySeq[r.Sequence]; ok && other != r.Tag {
			return fmt.Errorf("sequence %d is held by both %q and %q", r.Sequence, other, r.Tag)
		}
		bySeq[r.Sequence] = r.Tag
	}
	return nil
}
