// Package catalogue decides which listed tags enter a catalogue and in what
// order (design §3a rules 2-4, 6). It is pure: the only side effect is the
// injected Resolve callback. The reconciler and hack/parity both call Admit,
// so the parity tool exercises the controller's own admission code.
//
// Sequence numbers are the catalogue order and equal policy order:
//
//   - Incremental admission is tag-only. A listed tag that is not in the
//     catalogue and ranks above the catalogue's maximum under the policy is
//     admitted; tags below the maximum are never resolved by this step. They
//     are resolved oldest-first within the budget and numbered max+1, max+2, ...
//   - Backfill walks the listed tags below the catalogue's minimum, newest
//     first, while the catalogue does not yet reach back to the retention
//     window (or is empty). A tag is admitted iff its created time is not later
//     than that of every admitted tag above it ("it was the newest when it was
//     pushed"); a backport or a retag fails this and is excluded,
//     deterministically from registry state. Admitted tags are numbered
//     min-1, min-2, ... The walk stops when at least MinCount releases are
//     admitted and the last one is older than the retention cutoff, or when
//     the tags run out; the caller then records BackfillComplete.
//
// Releases without a created time (an image with no created annotation, or
// metadata that could not be resolved) are admitted by position alone: their
// created is firstSeen (createdObserved=true), which says nothing about when
// they were pushed, so they neither get rejected by the created rule nor
// tighten it for the tags below, and they never trigger the cutoff stop.
package catalogue

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/slicer"
	"github.com/kuberik/release-controller/internal/source"
)

// SequenceOrigin is the sequence of the first release of an empty catalogue.
// It sits mid-range so backfill can number downwards, and no value is zero.
const SequenceOrigin int64 = 1 << 32

// RejectedKey marks a permanent rejection in Release.Metadata.
const RejectedKey = slicer.RejectedKey

// Resolved is the outcome of resolving one tag.
type Resolved struct {
	Release source.Release
	// Rejected is non-empty for a permanent failure (bounds violation, policy
	// error). The release is stored once as unresolved + rejected, never retried.
	Rejected string
}

// ResolveFunc resolves one tag.
type ResolveFunc func(ref source.Ref) Resolved

// State is the catalogue as the slices hold it.
type State struct {
	// Catalogue is every stored release sorted by sequence ascending.
	Catalogue        []v1alpha1.Release
	BackfillComplete bool
}

// Config carries everything the decision depends on.
type Config struct {
	Less    func(a, b source.Release) bool
	Resolve ResolveFunc
	// Budget bounds the Resolve calls of one Admit.
	Budget int
	// Days is the retention window; 0 means the window never ends, so the
	// backfill walks every tag.
	Days     int32
	MinCount int
	Now      time.Time
	// WalkAfter resumes the backfill walk after this tag. It only saves
	// re-resolving tags the created rule already excluded; it is a hint, a
	// wrong or stale value costs requests, never correctness.
	WalkAfter string
	// Held reports a tag whose retry is backed off. A held tag stops the
	// backfill walk (order matters), it is not skipped.
	Held func(tag string) bool
}

// Rejection is a permanent rejection recorded in this Admit.
type Rejection struct{ Tag, Reason string }

// Result is what one Admit decided.
type Result struct {
	// Admitted are the new releases in admission order (incremental ascending,
	// then backfill descending) with Sequence, FirstSeen and Created set.
	Admitted []v1alpha1.Release
	// Resolves is the number of Resolve calls made.
	Resolves int
	// Leftover is the number of tags admission still has to resolve
	// (the budget ran out).
	Leftover int
	// BackfillComplete is the new value of status.backfillComplete.
	BackfillComplete bool
	// WalkPos is the last tag the backfill walk processed (for WalkAfter).
	WalkPos string
	// Rejections are the permanent rejections among Admitted.
	Rejections []Rejection
	// Retry are tags whose resolve failed transiently during the backfill walk
	// and were therefore not admitted; the caller backs them off.
	Retry []string
	// Waiting is true when the backfill walk stopped on a transient failure or
	// a backed-off tag rather than on the budget; the caller should requeue
	// after the backoff, not at once.
	Waiting bool
	// Transient are Admitted tags stored unresolved after a transient failure;
	// the caller schedules their repair.
	Transient []string
}

// Total order on releases: the policy, then the tag.
func lt(less func(a, b source.Release) bool, a, b source.Release) bool {
	if less(a, b) {
		return true
	}
	if less(b, a) {
		return false
	}
	return a.Tag < b.Tag
}

func stub(ref source.Ref) source.Release {
	if ref.Partial != nil {
		return *ref.Partial
	}
	return source.Release{Tag: ref.Tag}
}

func sec(t time.Time) time.Time { return t.UTC().Truncate(time.Second) }

// hasCreated reports whether a release carries a real created time.
func hasCreated(r v1alpha1.Release) bool {
	return !r.MetadataUnresolved && !r.CreatedObserved && !r.Created.IsZero()
}

// Admit decides one sync's admissions. refs is the whole listing.
func Admit(st State, refs []source.Ref, cfg Config) Result {
	var res Result
	cat := st.Catalogue
	inCat := make(map[string]struct{}, len(cat))
	for _, c := range cat {
		inCat[c.Tag] = struct{}{}
	}
	var max, min *v1alpha1.Release
	if len(cat) > 0 {
		min, max = &cat[0], &cat[len(cat)-1]
	}

	var ahead, below []source.Ref
	seen := map[string]struct{}{}
	for _, ref := range refs {
		if _, ok := inCat[ref.Tag]; ok {
			continue
		}
		if _, ok := seen[ref.Tag]; ok {
			continue
		}
		seen[ref.Tag] = struct{}{}
		s := stub(ref)
		switch {
		case max == nil:
			below = append(below, ref)
		case lt(cfg.Less, *max, s):
			ahead = append(ahead, ref)
		case lt(cfg.Less, s, *min):
			below = append(below, ref)
		}
		// else: inside the catalogue's range but absent: never admitted
		// (a retag/late tag, or a slice removed by hand, which is unsupported).
	}
	sort.SliceStable(ahead, func(i, j int) bool { return lt(cfg.Less, stub(ahead[i]), stub(ahead[j])) })
	sort.SliceStable(below, func(i, j int) bool { return lt(cfg.Less, stub(below[j]), stub(below[i])) })

	budget := cfg.Budget
	if budget <= 0 {
		budget = 1 << 30
	}
	// resolve calls the source and normalises the release; it records nothing.
	resolve := func(ref source.Ref) (v1alpha1.Release, string) {
		out := cfg.Resolve(ref)
		res.Resolves++
		rel := out.Release
		rel.Tag = ref.Tag
		if rel.FirstSeen.IsZero() {
			rel.FirstSeen = metav1.NewTime(cfg.Now)
		}
		if out.Rejected != "" {
			rel.MetadataUnresolved = true
			rel.Requires = nil
			rel.Metadata = map[string]string{RejectedKey: out.Rejected}
			rel.Created = metav1.Time{}
		}
		if rel.Created.IsZero() {
			rel.Created, rel.CreatedObserved = rel.FirstSeen, true
		}
		return rel, out.Rejected
	}
	// admit records an admission (rejections and transient stubs).
	admit := func(rel v1alpha1.Release, rejected string) {
		switch {
		case rejected != "":
			res.Rejections = append(res.Rejections, Rejection{rel.Tag, rejected})
		case rel.MetadataUnresolved:
			res.Transient = append(res.Transient, rel.Tag)
		}
		res.Admitted = append(res.Admitted, rel)
	}

	// Incremental: oldest first, so a partial batch never strands older ones.
	if max != nil {
		next := max.Sequence + 1
		for i, ref := range ahead {
			if res.Resolves >= budget {
				res.Leftover += len(ahead) - i
				break
			}
			rel, rj := resolve(ref)
			rel.Sequence = next
			next++
			admit(rel, rj)
		}
	}

	// Backfill.
	res.BackfillComplete = st.BackfillComplete && max != nil
	if len(refs) == 0 || (st.BackfillComplete && max != nil) {
		return res
	}
	var cutoff *time.Time
	if cfg.Days > 0 {
		c := cfg.Now.Add(-time.Duration(cfg.Days) * 24 * time.Hour)
		cutoff = &c
	}
	count := len(cat) + len(res.Admitted)
	var runMin *time.Time // earliest created among admitted real-created releases (all above the walk)
	var lowest *time.Time // created of the lowest-sequence admitted real-created release
	consider := func(r v1alpha1.Release) {
		if !hasCreated(r) {
			return
		}
		c := sec(r.Created.Time)
		if runMin == nil || c.Before(*runMin) {
			runMin = &c
		}
	}
	for _, c := range cat {
		consider(c)
	}
	for _, a := range res.Admitted {
		consider(a)
	}
	for _, c := range cat { // lowest sequence first
		if hasCreated(c) {
			t := sec(c.Created.Time)
			lowest = &t
			break
		}
	}
	pastWindow := func(created *time.Time) bool {
		return cutoff != nil && count >= cfg.MinCount && created != nil && created.Before(*cutoff)
	}
	if max != nil && pastWindow(lowest) { // the catalogue already reaches back far enough
		res.BackfillComplete = true
		return res
	}
	nextLow := SequenceOrigin
	if min != nil {
		nextLow = min.Sequence - 1
	}
	start := 0
	if cfg.WalkAfter != "" {
		for i, ref := range below {
			if ref.Tag == cfg.WalkAfter {
				start = i + 1
				break
			}
		}
	}
	done := true
	for i := start; i < len(below); i++ {
		ref := below[i]
		if res.Resolves >= budget {
			res.Leftover += len(below) - i
			done = false
			break
		}
		if cfg.Held != nil && cfg.Held(ref.Tag) {
			res.Waiting, done = true, false
			break
		}
		rel, rj := resolve(ref)
		res.WalkPos = ref.Tag
		if rel.MetadataUnresolved && rj == "" {
			// Transient failure: without created the rule cannot be applied,
			// and skipping would lose the tag's place. Stop and retry later.
			res.Retry = append(res.Retry, ref.Tag)
			res.Waiting, done = true, false
			break
		}
		real := hasCreated(rel)
		if real && runMin != nil && sec(rel.Created.Time).After(*runMin) {
			continue // created later than a tag above it: a backport or retag
		}
		rel.Sequence = nextLow
		nextLow--
		admit(rel, rj)
		count++
		if real {
			c := sec(rel.Created.Time)
			runMin = &c
			if pastWindow(&c) {
				break
			}
		}
	}
	res.BackfillComplete = done
	return res
}
