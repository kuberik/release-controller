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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	kuberikcomv1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
	cat "github.com/kuberik/release-controller/internal/catalogue"
	"github.com/kuberik/release-controller/internal/slicer"
	"github.com/kuberik/release-controller/internal/source"
)

const (
	// IdentityAnnotation is set on every slice and records the spec identity
	// (oci.image or imagePolicyRef name) the catalogue was built from.
	IdentityAnnotation = "kuberik.com/source-identity"

	lastSyncWriteEvery = 5 * time.Minute
	defaultInterval    = time.Minute
	defaultResolves    = 50
	syncingRequeue     = time.Second
)

// ReleaseSourceReconciler reconciles a ReleaseSource object
type ReleaseSourceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// MaxReleasesPerSlice seals a slice at this many releases (1-1000).
	MaxReleasesPerSlice int
	// MaxSliceBytes seals a slice early once its encoded size reaches this.
	MaxSliceBytes int64
	// MaxResolvesPerSync bounds metadata resolutions per sync.
	MaxResolvesPerSync int
	// MaxConcurrentReconciles bounds concurrent ReleaseSource reconciles.
	MaxConcurrentReconciles int

	// SourceFactory builds the Source for a ReleaseSource. Nil means
	// source.Lookup("oci"). Tests inject a fake.
	SourceFactory source.Factory
	// APIReader is an uncached reader handed to the source factory, so the
	// manager starts no informers for ImagePolicy/ImageRepository/Secret.
	// Nil falls back to Client (tests).
	APIReader client.Reader
	// Recorder emits Events; defaulted in SetupWithManager, nil-safe.
	Recorder record.EventRecorder
	// Now is the clock; nil means time.Now.
	Now func() time.Time

	// mem is per-source memory that only saves work and noise (retry
	// backoff, the backfill walk position, the last TagsSkipped count). Losing
	// it on restart costs a few requests or one repeated Event, never
	// correctness: admission is decided from the catalogue and the listing.
	memMu sync.Mutex
	mem   map[types.UID]*sourceMem
}

type backoffEntry struct {
	attempts int
	next     time.Time
}

type sourceMem struct {
	backoff     map[string]backoffEntry
	walk        cat.WalkMemory // backfill walk position; committed only after the slice write succeeded
	lastSkipped int
	// blockedID is the identity whose change was reported as blocked, so the
	// Warning fires on the transition, not on every retry.
	blockedID string
	// unresolvable are backfill tags already reported as TagUnresolvable.
	unresolvable map[string]bool
}

const (
	backoffInitial = 10 * time.Second
	backoffMax     = time.Hour
)

// +kubebuilder:rbac:groups=kuberik.com,resources=releasesources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kuberik.com,resources=releasesources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kuberik.com,resources=releasesources/finalizers,verbs=update
// +kubebuilder:rbac:groups=kuberik.com,resources=releaseslices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=image.toolkit.fluxcd.io,resources=imagepolicies;imagerepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *ReleaseSourceReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ReleaseSourceReconciler) event(obj runtime.Object, typ, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(obj, typ, reason, msg)
	}
}

func (r *ReleaseSourceReconciler) memFor(uid types.UID) *sourceMem {
	r.memMu.Lock()
	defer r.memMu.Unlock()
	if r.mem == nil {
		r.mem = map[types.UID]*sourceMem{}
	}
	m := r.mem[uid]
	if m == nil {
		m = &sourceMem{backoff: map[string]backoffEntry{}}
		r.mem[uid] = m
	}
	return m
}

func (r *ReleaseSourceReconciler) forgetMem(uid types.UID) {
	r.memMu.Lock()
	defer r.memMu.Unlock()
	delete(r.mem, uid)
}

// failed registers a failed resolve of tag and returns when to retry:
// exponential from 10 s, capped at 1 h.
func (m *sourceMem) failed(tag string, now time.Time) {
	e := m.backoff[tag]
	e.attempts++
	d := backoffInitial << (e.attempts - 1)
	if d > backoffMax || d <= 0 {
		d = backoffMax
	}
	e.next = now.Add(d)
	m.backoff[tag] = e
}

func (m *sourceMem) held(tag string, now time.Time) bool {
	e, ok := m.backoff[tag]
	return ok && now.Before(e.next)
}

// identity is what the catalogue was built from: the spec's image or
// imagePolicyRef name and, for a reference, the repository the referenced
// objects resolve to now (a changed ImageRepository image is a new catalogue).
func identity(rs *kuberikcomv1alpha1.ReleaseSource, src source.Source) string {
	o := rs.Spec.OCI
	if o == nil {
		return ""
	}
	var id string
	if o.ImagePolicyRef != nil {
		id = "imagePolicyRef:" + o.ImagePolicyRef.Name
		if i, ok := src.(source.Identifier); ok && i.Identity() != "" {
			id += "@" + i.Identity()
		}
	} else {
		id = "image:" + o.Image
	}
	// The effective policy, filterTags and exclusion list decide the order the
	// stored sequences encode: a change is a new catalogue.
	if f, ok := src.(source.Fingerprinter); ok && f.Fingerprint() != "" {
		id += "#" + f.Fingerprint()
	}
	return id
}

func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.9 + 0.2*rand.Float64()))
}

func rejectedTag(r kuberikcomv1alpha1.Release) bool {
	_, ok := r.Metadata[slicer.RejectedKey]
	return ok
}

// Reconcile runs one sync of a ReleaseSource (design §3, §3a).
func (r *ReleaseSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rs kuberikcomv1alpha1.ReleaseSource
	if err := r.Get(ctx, req.NamespacedName, &rs); err != nil {
		if apierrors.IsNotFound(err) {
			forgetMetrics(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !rs.DeletionTimestamp.IsZero() || rs.Spec.Suspend {
		forgetMetrics(rs.Namespace, rs.Name)
		if !rs.DeletionTimestamp.IsZero() {
			r.forgetMem(rs.UID)
		}
		return ctrl.Result{}, nil
	}
	defer observeSync(&rs, time.Now())
	interval := defaultInterval
	if rs.Spec.Interval != nil {
		interval = rs.Spec.Interval.Duration
	}
	if interval <= 0 {
		interval = defaultInterval
	}
	now := r.now()
	st := rs.Status.DeepCopy()
	st.ObservedGeneration = rs.Generation
	mem := r.memFor(rs.UID)

	existing, err := r.listSlices(ctx, &rs)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The new configuration must build before anything is deleted.
	factory := r.SourceFactory
	if factory == nil {
		var ferr error
		if factory, ferr = source.Lookup("oci"); ferr != nil {
			return r.fail(ctx, &rs, st, interval, kuberikcomv1alpha1.ReadyCondition, "ConfigInvalid", ferr.Error())
		}
	}
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	src, err := factory(ctx, reader, &rs)
	if err != nil {
		r.event(&rs, "Warning", "ConfigInvalid", err.Error())
		return r.fail(ctx, &rs, st, interval, kuberikcomv1alpha1.ReadyCondition, "ConfigInvalid", err.Error())
	}

	// Identity change: the catalogue belongs to a different source. Validate
	// the new one with a real listing first; a broken new config keeps the
	// old catalogue serving.
	id := identity(&rs, src)
	var pre *source.DiscoverResult
	for _, s := range existing {
		old, ok := s.Annotations[IdentityAnnotation]
		if !ok || old == id {
			continue
		}
		dres, derr := src.Discover(ctx, "")
		if derr != nil {
			if mem.blockedID != id {
				mem.blockedID = id
				r.event(&rs, "Warning", "IdentityChangeBlocked", fmt.Sprintf("source identity changed from %q to %q but the new source cannot be listed, keeping the old catalogue: %v", old, id, derr))
			}
			return r.unreachable(ctx, &rs, st, now, interval, true, derr)
		}
		pre = &dres
		// Persist the reset before any slice is deleted or written: a rebuild
		// that dies after its first batch must not leave a stored
		// backfillComplete=true next to a part-built catalogue.
		st.Cursor, st.Newest, st.Artifact, st.Count, st.SliceCount, st.OpenSlice, st.BackfillComplete = "", nil, nil, 0, 0, "", false
		if err := r.persist(ctx, &rs, st); err != nil {
			return ctrl.Result{}, err
		}
		for i := range existing {
			if err := r.Delete(ctx, &existing[i]); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.event(&rs, "Normal", "IdentityChanged", fmt.Sprintf("source identity changed from %q to %q; catalogue rebuilt", old, id))
		existing = nil
		mem.backoff, mem.walk = map[string]backoffEntry{}, cat.WalkMemory{}
		break
	}

	// A catalogue that is empty at sync start (first sync, or every slice
	// deleted to rebuild) is rebuilt from scratch: the stored progress must say
	// so before the first slice is written.
	if len(existing) == 0 && (st.BackfillComplete || st.Count != 0 || st.SliceCount != 0 || st.OpenSlice != "" || st.Newest != nil) {
		st.Cursor, st.Newest, st.Artifact, st.Count, st.SliceCount, st.OpenSlice, st.BackfillComplete = "", nil, nil, 0, 0, "", false
		if err := r.persist(ctx, &rs, st); err != nil {
			return ctrl.Result{}, err
		}
		mem.walk = cat.WalkMemory{}
	}

	mem.blockedID = ""
	catalogue := slicer.Merge(existing)
	repairable := 0
	for _, c := range catalogue {
		if c.MetadataUnresolved && !rejectedTag(c) {
			repairable++
		}
	}

	// 1. Discover. While work is pending, ignore the cursor.
	var dres source.DiscoverResult
	if pre != nil {
		dres = *pre
	} else {
		cursor := st.Cursor
		if meta.IsStatusConditionTrue(st.Conditions, kuberikcomv1alpha1.SyncingCondition) {
			cursor = ""
		}
		var derr error
		dres, derr = src.Discover(ctx, cursor)
		if derr != nil {
			log.Info("discover failed", "error", derr.Error())
			return r.unreachable(ctx, &rs, st, now, interval, len(existing) > 0, derr)
		}
	}
	if ts, ok := src.(source.TagSkipper); ok {
		n := ts.SkippedLongTags()
		if n > 0 && n != mem.lastSkipped {
			r.event(&rs, "Warning", "TagsSkipped", fmt.Sprintf("%d tags skipped: longer than 128 characters", n))
		}
		mem.lastSkipped = n
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SourceReachableCondition, Status: metav1.ConditionTrue, Reason: "Reachable", Message: "source listed", ObservedGeneration: rs.Generation})

	if dres.NotModified && repairable == 0 && st.BackfillComplete && len(catalogue) > 0 {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SyncingCondition, Status: metav1.ConditionFalse, Reason: "Idle", Message: "nothing to resolve", ObservedGeneration: rs.Generation})
		r.setReady(st, rs.Generation, true, "Synced", "catalogue is current")
		if st.LastSyncTime == nil || now.Sub(st.LastSyncTime.Time) >= lastSyncWriteEvery {
			st.LastSyncTime = &metav1.Time{Time: now}
		}
		return r.finish(ctx, &rs, st, interval, 0)
	}

	budget := r.MaxResolvesPerSync
	if budget <= 0 {
		budget = defaultResolves
	}
	retDays, minCount := retention(&rs)
	resolver := func(ref source.Ref) cat.Resolved {
		rel, rerr := src.Resolve(ctx, ref)
		if rerr != nil {
			resolveErrors.WithLabelValues(sourceKind(&rs), rs.Namespace, rs.Name).Inc()
			log.Info("resolve failed", "tag", ref.Tag, "error", rerr.Error())
			return cat.Resolved{Release: kuberikcomv1alpha1.Release{Tag: ref.Tag, MetadataUnresolved: true}, Rejected: rerr.Error()}
		}
		out := cat.Resolved{Release: rel}
		if rej, ok := src.(source.Rejecter); ok {
			if msg, rejected := rej.Rejection(ref.Tag); rejected {
				out.Rejected = msg
			}
		}
		if rel.MetadataUnresolved {
			resolveErrors.WithLabelValues(sourceKind(&rs), rs.Namespace, rs.Name).Inc()
		}
		return out
	}

	// 2. Repairs: unresolved catalogue entries, per-tag backoff, at most a
	// quarter of the budget so they cannot starve admission.
	refByTag := map[string]source.Ref{}
	for _, ref := range dres.Refs {
		refByTag[ref.Tag] = ref
	}
	var repairs []kuberikcomv1alpha1.Release
	repairBudget := budget / 4
	if repairBudget < 1 {
		repairBudget = 1
	}
	used, repairLeft := 0, 0
	for i := len(catalogue) - 1; i >= 0; i-- { // newest first
		c := catalogue[i]
		if !c.MetadataUnresolved || rejectedTag(c) {
			continue
		}
		if mem.held(c.Tag, now) {
			continue
		}
		if used >= repairBudget {
			repairLeft++
			continue
		}
		ref, ok := refByTag[c.Tag]
		if !ok {
			ref = source.Ref{Tag: c.Tag}
		}
		out := resolver(ref)
		used++
		rel := out.Release
		rel.Tag = c.Tag
		switch {
		case out.Rejected != "":
			rel.MetadataUnresolved, rel.Requires, rel.Created = true, nil, metav1.Time{}
			rel.Metadata = map[string]string{slicer.RejectedKey: out.Rejected}
			repairs = append(repairs, rel)
			delete(mem.backoff, c.Tag)
			r.event(&rs, "Warning", "MetadataRejected", fmt.Sprintf("%s: %s", c.Tag, out.Rejected))
		case rel.MetadataUnresolved:
			mem.failed(c.Tag, now)
		default:
			repairs = append(repairs, rel)
			delete(mem.backoff, c.Tag)
		}
	}

	// 3. Admission: incremental above the catalogue maximum, then backfill
	// below its minimum (shared with hack/parity).
	minTag := ""
	if len(catalogue) > 0 {
		minTag = catalogue[0].Tag
	}
	walkAfter := mem.walk.After(minTag)
	admitBudget := budget - used
	if admitBudget <= 0 {
		admitBudget = -1 // spent by repairs; 0 would mean unlimited
	}
	res := cat.Admit(cat.State{Catalogue: catalogue, BackfillComplete: st.BackfillComplete}, dres.Refs, cat.Config{
		Less:      src.Less,
		Resolve:   resolver,
		Budget:    admitBudget,
		Days:      retDays,
		MinCount:  int(minCount),
		Now:       now,
		WalkAfter: walkAfter,
		Held:      func(tag string) bool { return mem.held(tag, now) },
		Attempts:  func(tag string) int { return mem.backoff[tag].attempts },
	})
	for _, tag := range res.Transient {
		mem.failed(tag, now)
	}
	for _, tag := range res.Retry {
		mem.failed(tag, now)
	}
	for _, tag := range res.Unresolvable {
		if mem.unresolvable == nil {
			mem.unresolvable = map[string]bool{}
		}
		if !mem.unresolvable[tag] {
			mem.unresolvable[tag] = true
			r.event(&rs, "Warning", "TagUnresolvable", fmt.Sprintf("%s: metadata could not be resolved after %d attempts; excluded from the backfilled catalogue", tag, cat.MaxAttempts))
		}
	}
	for _, rj := range res.Rejections {
		r.event(&rs, "Warning", "MetadataRejected", fmt.Sprintf("%s: %s", rj.Tag, rj.Reason))
	}
	leftover := res.Leftover + repairLeft

	// 4. Plan and apply.
	cfg := slicer.Config{
		Namespace:     rs.Namespace,
		Source:        rs.Name,
		Owner:         *metav1.NewControllerRef(&rs, kuberikcomv1alpha1.GroupVersion.WithKind("ReleaseSource")),
		Annotations:   map[string]string{IdentityAnnotation: id},
		NextIndex:     st.NextSliceIndex,
		Now:           now,
		RetentionDays: retDays,
		MinCount:      int(minCount),
		MaxPerSlice:   r.MaxReleasesPerSlice,
		MaxBytes:      r.MaxSliceBytes,
	}
	plan := slicer.Compute(slicer.Input{Existing: existing, New: res.Admitted, Repairs: repairs}, cfg)
	for _, tag := range plan.RepairsRejected {
		delete(mem.backoff, tag)
		r.event(&rs, "Warning", "MetadataRejected", fmt.Sprintf("%s: resolved metadata would grow its slice over the size limit", tag))
	}
	if err := slicer.CheckSequences(plan.Final); err != nil {
		// Surface the refusal: Ready=False, one Warning on the transition,
		// nothing written. Retry soon (a stale list can clear on its own).
		if c := meta.FindStatusCondition(st.Conditions, kuberikcomv1alpha1.ReadyCondition); c == nil || c.Reason != "SequenceCollision" {
			r.event(&rs, "Warning", "SequenceCollision", err.Error())
		}
		r.setReady(st, rs.Generation, false, "SequenceCollision", "refusing to write: "+err.Error())
		retry := interval
		if retry > time.Minute {
			retry = time.Minute
		}
		return r.finish(ctx, &rs, st, interval, retry)
	}
	if conflict, err := r.apply(ctx, rs.Namespace, plan); err != nil {
		return ctrl.Result{}, err
	} else if conflict {
		return ctrl.Result{RequeueAfter: syncingRequeue}, nil
	}

	// 5. Status from the post-plan state. The walk memory advances only now:
	// the batch it skips past is stored.
	final := slicer.Merge(plan.Final)
	newMin := ""
	if len(final) > 0 {
		newMin = final[0].Tag
	}
	mem.walk = mem.walk.Next(walkAfter, res, newMin)
	st.Count = int32(len(final))
	releasesGauge.WithLabelValues(sourceKind(&rs), rs.Namespace, rs.Name).Set(float64(len(final)))
	slicesGauge.WithLabelValues(sourceKind(&rs), rs.Namespace, rs.Name).Set(float64(len(plan.Final)))
	st.SliceCount = int32(len(plan.Final))
	st.OpenSlice = ""
	for _, s := range plan.Final {
		if !s.Sealed {
			st.OpenSlice = s.Name
		}
	}
	st.NextSliceIndex = plan.NextIndex
	st.BackfillComplete = res.BackfillComplete
	if len(final) == 0 {
		st.Newest, st.Artifact = nil, nil
	} else {
		newest := final[len(final)-1] // highest sequence = newest under the policy
		if st.Newest == nil || st.Newest.Tag != newest.Tag || st.Artifact == nil {
			if ap, ok := src.(source.ArtifactProvider); ok {
				if a, aerr := ap.Artifact(ctx, newest); aerr == nil && a != nil {
					st.Artifact = a
				}
			}
		}
		created := newest.Created
		ref := &kuberikcomv1alpha1.ReleaseRef{Tag: newest.Tag, Created: &created}
		if newest.Revision != nil {
			ref.Revision = *newest.Revision
		}
		st.Newest = ref
	}
	pending := leftover > 0 || res.Waiting
	if !pending {
		st.Cursor = dres.Cursor
	}
	if pending {
		reason, msg := "ResolveBudget", fmt.Sprintf("%d tags left to resolve", leftover)
		if res.Waiting && leftover == 0 {
			reason, msg = "Backoff", "waiting to retry a tag that failed to resolve"
		}
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SyncingCondition, Status: metav1.ConditionTrue, Reason: reason, Message: msg, ObservedGeneration: rs.Generation})
	} else {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SyncingCondition, Status: metav1.ConditionFalse, Reason: "Idle", Message: "nothing to resolve", ObservedGeneration: rs.Generation})
	}
	r.setReady(st, rs.Generation, true, "Synced", "catalogue is current")
	if !plan.Empty() || st.LastSyncTime == nil || now.Sub(st.LastSyncTime.Time) >= lastSyncWriteEvery {
		st.LastSyncTime = &metav1.Time{Time: now}
	}

	// Requeue: at once only while budget-limited admission made progress;
	// otherwise at the soonest retry of a backed-off tag, never sooner than
	// 1 s and never later than the interval.
	if leftover > 0 && (len(res.Admitted) > 0 || len(repairs) > 0) {
		return r.finish(ctx, &rs, st, interval, syncingRequeue)
	}
	var soonest time.Time
	for _, c := range final {
		if c.MetadataUnresolved && !rejectedTag(c) {
			if e, ok := mem.backoff[c.Tag]; ok && (soonest.IsZero() || e.next.Before(soonest)) {
				soonest = e.next
			}
		}
	}
	for _, tag := range res.Retry {
		if e, ok := mem.backoff[tag]; ok && (soonest.IsZero() || e.next.Before(soonest)) {
			soonest = e.next
		}
	}
	if !soonest.IsZero() {
		d := soonest.Sub(now)
		if d < time.Second {
			d = time.Second
		}
		if d < interval {
			return r.finish(ctx, &rs, st, interval, d)
		}
	}
	return r.finish(ctx, &rs, st, interval, 0)
}

func retention(rs *kuberikcomv1alpha1.ReleaseSource) (days int32, minCount int32) {
	days, minCount = 7, 30
	if rs.Spec.Retention.Days != nil {
		days = *rs.Spec.Retention.Days
	}
	if rs.Spec.Retention.MinCount != nil {
		minCount = *rs.Spec.Retention.MinCount
	}
	return
}

// listSlices reads the source's slices through the uncached API reader (a
// handful of objects): admission numbers sequences from the truth, not from an
// informer that may not have seen the previous write yet.
func (r *ReleaseSourceReconciler) listSlices(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource) ([]kuberikcomv1alpha1.ReleaseSlice, error) {
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var list kuberikcomv1alpha1.ReleaseSliceList
	if err := reader.List(ctx, &list, client.InNamespace(rs.Namespace), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: rs.Name}); err != nil {
		return nil, err
	}
	var out []kuberikcomv1alpha1.ReleaseSlice
	for _, s := range list.Items {
		if metav1.IsControlledBy(&s, rs) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// apply writes the plan: updates first (the seal), then creates, then
// deletes. A crash between update and create leaves overflow releases out of
// the catalogue; they are "new" again next sync, never duplicated. conflict
// reports a stale-cache conflict to retry.
func (r *ReleaseSourceReconciler) apply(ctx context.Context, ns string, p slicer.Plan) (conflict bool, err error) {
	for i := range p.Updates {
		if err := r.Update(ctx, &p.Updates[i]); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
	}
	for i := range p.Creates {
		if err := r.Create(ctx, &p.Creates[i]); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return true, nil
			}
			return false, err
		}
	}
	for _, name := range p.Deletes {
		obj := &kuberikcomv1alpha1.ReleaseSlice{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return false, nil
}

func (r *ReleaseSourceReconciler) setReady(st *kuberikcomv1alpha1.ReleaseSourceStatus, gen int64, ok bool, reason, msg string) {
	status := metav1.ConditionTrue
	if !ok {
		status = metav1.ConditionFalse
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.ReadyCondition, Status: status, Reason: reason, Message: msg, ObservedGeneration: gen})
}

// persist writes status now (rebuild start) so later slice writes are never
// ahead of the stored progress. A failure, a conflict included, is returned.
func (r *ReleaseSourceReconciler) persist(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource, st *kuberikcomv1alpha1.ReleaseSourceStatus) error {
	if apiequality.Semantic.DeepEqual(rs.Status, *st) {
		return nil
	}
	rs.Status = *st.DeepCopy()
	return r.Status().Update(ctx, rs)
}

// finish writes status only if it changed and schedules the next sync.
func (r *ReleaseSourceReconciler) finish(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource, st *kuberikcomv1alpha1.ReleaseSourceStatus, interval, requeue time.Duration) (ctrl.Result, error) {
	if !apiequality.Semantic.DeepEqual(rs.Status, *st) {
		rs.Status = *st
		if err := r.Status().Update(ctx, rs); err != nil {
			return ctrl.Result{}, err
		}
	}
	if requeue > 0 {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	return ctrl.Result{RequeueAfter: jitter(interval)}, nil
}

// fail records bad config: Ready=False, no slice writes.
func (r *ReleaseSourceReconciler) fail(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource, st *kuberikcomv1alpha1.ReleaseSourceStatus, interval time.Duration, _ string, reason, msg string) (ctrl.Result, error) {
	r.setReady(st, rs.Generation, false, reason, msg)
	return r.finish(ctx, rs, st, interval, 0)
}

// unreachable keeps the catalogue. Ready stays True (reason Stale) until
// 3 x interval after the first failure, then goes False. The failure start is
// the SourceReachable condition's transition time, which is write-free.
func (r *ReleaseSourceReconciler) unreachable(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource, st *kuberikcomv1alpha1.ReleaseSourceStatus, now time.Time, interval time.Duration, haveCatalogue bool, derr error) (ctrl.Result, error) {
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SourceReachableCondition, Status: metav1.ConditionFalse, Reason: "Unreachable", Message: derr.Error(), ObservedGeneration: rs.Generation})
	c := meta.FindStatusCondition(st.Conditions, kuberikcomv1alpha1.SourceReachableCondition)
	since := c.LastTransitionTime.Time
	if haveCatalogue && now.Sub(since) < 3*interval {
		r.setReady(st, rs.Generation, true, "Stale", "source unreachable; serving the last catalogue")
	} else {
		r.setReady(st, rs.Generation, false, "SourceUnreachable", derr.Error())
	}
	return r.finish(ctx, rs, st, interval, 0)
}

// SetupWithManager sets up the controller with the Manager. Owned slices are
// watched for deletion only: a deleted slice must be re-filled, while our own
// creates and updates must not trigger a registry listing.
func (r *ReleaseSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("release-controller") //nolint:staticcheck // SA1019: Recorder is a record.EventRecorder; moving to the events.k8s.io API also changes the RBAC the chart grants
	}
	deleteOnly := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
	}
	conc := r.MaxConcurrentReconciles
	if conc <= 0 {
		conc = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&kuberikcomv1alpha1.ReleaseSource{}, builder.WithPredicates(sourceChangedPredicate())).
		Owns(&kuberikcomv1alpha1.ReleaseSlice{}, builder.WithPredicates(deleteOnly)).
		WithOptions(controller.Options{MaxConcurrentReconciles: conc}).
		Named("releasesource").
		Complete(r)
}

// sourceChangedPredicate lets spec (generation) and annotation changes of a
// ReleaseSource through but not status-only updates: the reconciler writes its
// own status, and a registry listing per status write would loop.
func sourceChangedPredicate() predicate.Predicate {
	return predicate.Or(predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{})
}
