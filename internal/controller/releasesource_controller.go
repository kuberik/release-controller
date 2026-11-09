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
	// Recorder emits Events; defaulted in SetupWithManager, nil-safe.
	Recorder record.EventRecorder
	// Now is the clock; nil means time.Now.
	Now func() time.Time

	// skipped remembers tags the reconciler decided not to admit (not
	// monotonic, or past the first-sync cutoff) so they are not re-resolved on
	// every sync. Keyed by source UID and generation; memory only, a restart
	// just resolves them once more.
	skipMu  sync.Mutex
	skipped map[skipKey]map[string]struct{}
}

type skipKey struct {
	uid types.UID
	gen int64
}

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

func identity(rs *kuberikcomv1alpha1.ReleaseSource) string {
	if o := rs.Spec.OCI; o != nil {
		if o.ImagePolicyRef != nil {
			return "imagePolicyRef:" + o.ImagePolicyRef.Name
		}
		return "image:" + o.Image
	}
	return ""
}

func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.9 + 0.2*rand.Float64()))
}

// Reconcile runs one sync of a ReleaseSource (design §3 "Sync").
func (r *ReleaseSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rs kuberikcomv1alpha1.ReleaseSource
	if err := r.Get(ctx, req.NamespacedName, &rs); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !rs.DeletionTimestamp.IsZero() || rs.Spec.Suspend {
		return ctrl.Result{}, nil
	}
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

	existing, err := r.listSlices(ctx, &rs)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Identity change: the catalogue belongs to a different source.
	id := identity(&rs)
	for _, s := range existing {
		if old, ok := s.Annotations[IdentityAnnotation]; ok && old != id {
			for i := range existing {
				if err := r.Delete(ctx, &existing[i]); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			r.event(&rs, "Normal", "IdentityChanged", fmt.Sprintf("source identity changed from %q to %q; catalogue rebuilt", old, id))
			existing = nil
			st.Cursor, st.Newest, st.Artifact, st.Count, st.Slices = "", nil, nil, 0, nil
			r.forgetSkipped(rs.UID)
			break
		}
	}

	factory := r.SourceFactory
	if factory == nil {
		var ferr error
		if factory, ferr = source.Lookup("oci"); ferr != nil {
			return r.fail(ctx, &rs, st, now, interval, kuberikcomv1alpha1.ReadyCondition, "ConfigInvalid", ferr.Error())
		}
	}
	src, err := factory(ctx, r.Client, &rs)
	if err != nil {
		r.event(&rs, "Warning", "ConfigInvalid", err.Error())
		return r.fail(ctx, &rs, st, now, interval, kuberikcomv1alpha1.ReadyCondition, "ConfigInvalid", err.Error())
	}

	catalogue := slicer.Merge(existing)
	inCat := map[string]kuberikcomv1alpha1.Release{}
	for _, c := range catalogue {
		inCat[c.Tag] = c
	}
	unresolved := 0
	for _, c := range catalogue {
		if c.MetadataUnresolved {
			unresolved++
		}
	}

	// 1. Discover. While a resolve backlog exists, ignore the cursor.
	cursor := st.Cursor
	if meta.IsStatusConditionTrue(st.Conditions, kuberikcomv1alpha1.SyncingCondition) {
		cursor = ""
	}
	dres, derr := src.Discover(ctx, cursor)
	if derr != nil {
		log.Info("discover failed", "error", derr.Error())
		return r.unreachable(ctx, &rs, st, now, interval, len(existing) > 0, derr)
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SourceReachableCondition, Status: metav1.ConditionTrue, Reason: "Reachable", Message: "source listed", ObservedGeneration: rs.Generation})

	if dres.NotModified && unresolved == 0 {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SyncingCondition, Status: metav1.ConditionFalse, Reason: "Idle", Message: "nothing to resolve", ObservedGeneration: rs.Generation})
		r.setReady(st, rs.Generation, true, "Synced", "catalogue is current")
		if st.LastSyncTime == nil || now.Sub(st.LastSyncTime.Time) >= lastSyncWriteEvery {
			st.LastSyncTime = &metav1.Time{Time: now}
		}
		return r.finish(ctx, &rs, st, interval, 0)
	}

	// 2. new = discovered - catalogue (+ unresolved).
	skip := r.skippedFor(&rs)
	var resolveSet []source.Ref
	refByTag := map[string]source.Ref{}
	for _, ref := range dres.Refs {
		refByTag[ref.Tag] = ref
		if _, ok := inCat[ref.Tag]; ok {
			continue
		}
		if _, ok := skip[ref.Tag]; ok {
			continue
		}
		resolveSet = append(resolveSet, ref)
	}
	repairTags := map[string]bool{}
	for _, c := range catalogue {
		if c.MetadataUnresolved {
			repairTags[c.Tag] = true
			ref, ok := refByTag[c.Tag]
			if !ok {
				ref = source.Ref{Tag: c.Tag}
			}
			resolveSet = append(resolveSet, ref)
		}
	}

	// 3. Newest first by the source's order, bounded.
	stub := func(ref source.Ref) kuberikcomv1alpha1.Release {
		if ref.Partial != nil {
			return *ref.Partial
		}
		return kuberikcomv1alpha1.Release{Tag: ref.Tag}
	}
	sort.SliceStable(resolveSet, func(i, j int) bool {
		a, b := stub(resolveSet[i]), stub(resolveSet[j])
		if src.Less(a, b) {
			return false
		}
		if src.Less(b, a) {
			return true
		}
		return a.Tag > b.Tag
	})
	budget := r.MaxResolvesPerSync
	if budget <= 0 {
		budget = defaultResolves
	}
	leftover := 0
	if len(resolveSet) > budget {
		leftover = len(resolveSet) - budget
		resolveSet = resolveSet[:budget]
	}

	retDays, minCount := retention(&rs)
	cutoff := now.Add(-time.Duration(retDays) * 24 * time.Hour)
	var candidates, repairs []kuberikcomv1alpha1.Release
	for i, ref := range resolveSet {
		rel, rerr := src.Resolve(ctx, ref)
		if rerr != nil {
			log.Info("resolve failed", "tag", ref.Tag, "error", rerr.Error())
			r.event(&rs, "Warning", "MetadataRejected", fmt.Sprintf("%s: %v", ref.Tag, rerr))
			continue
		}
		if rel.Tag == "" {
			rel.Tag = ref.Tag
		}
		if repairTags[ref.Tag] {
			if !rel.MetadataUnresolved {
				repairs = append(repairs, rel)
			}
			continue
		}
		if rel.FirstSeen.IsZero() {
			rel.FirstSeen = metav1.NewTime(now)
		}
		if rel.Created.IsZero() {
			rel.Created, rel.CreatedObserved = rel.FirstSeen, true
		}
		candidates = append(candidates, rel)
		// First sync of a big repo: once minCount releases are in hand and
		// this one is already past retention, the rest would be dropped anyway.
		if len(existing) == 0 && retDays > 0 && len(candidates) >= int(minCount) && rel.Created.Time.Before(cutoff) {
			for _, rest := range resolveSet[i+1:] {
				if !repairTags[rest.Tag] {
					r.markSkipped(&rs, rest.Tag)
				}
			}
			leftover = 0
			break
		}
	}

	// 4. Admission.
	admitAll := false
	if a, ok := src.(source.AdmitAll); ok {
		admitAll = a.AdmitAll()
	}
	admitted, rejected := slicer.Admit(catalogue, candidates, src.Less, admitAll)
	// Expired tags that retention already dropped (or would drop at once) are
	// still listed by the source; admitting them would make retention and
	// admission fight forever. Keep them out unless minCount needs them.
	admitted, expired := slicer.DropExpired(len(catalogue), admitted, now, retDays, int(minCount))
	rejected = append(rejected, expired...)
	for _, rej := range rejected {
		r.markSkipped(&rs, rej.Tag)
	}

	// 5-7. Plan and apply.
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
	plan := slicer.Compute(slicer.Input{Existing: existing, New: admitted, Repairs: repairs}, cfg)
	if conflict, err := r.apply(ctx, rs.Namespace, plan); err != nil {
		return ctrl.Result{}, err
	} else if conflict {
		return ctrl.Result{RequeueAfter: syncingRequeue}, nil
	}

	// 8. Status from the post-plan state.
	final := slicer.Merge(plan.Final)
	st.Count = int32(len(final))
	st.Slices = nil
	for _, s := range plan.Final {
		st.Slices = append(st.Slices, s.Name)
	}
	st.NextSliceIndex = plan.NextIndex
	var newest *kuberikcomv1alpha1.Release
	for i := range final {
		if newest == nil || src.Less(*newest, final[i]) {
			newest = &final[i]
		}
	}
	if newest == nil {
		st.Newest, st.Artifact = nil, nil
	} else {
		if st.Newest == nil || st.Newest.Tag != newest.Tag || st.Artifact == nil {
			if ap, ok := src.(source.ArtifactProvider); ok {
				if a, aerr := ap.Artifact(ctx, *newest); aerr == nil && a != nil {
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
	if leftover == 0 {
		st.Cursor = dres.Cursor
	}
	if leftover > 0 {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SyncingCondition, Status: metav1.ConditionTrue, Reason: "ResolveBudget", Message: fmt.Sprintf("%d tags left to resolve", leftover), ObservedGeneration: rs.Generation})
	} else {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{LastTransitionTime: metav1.NewTime(r.now()), Type: kuberikcomv1alpha1.SyncingCondition, Status: metav1.ConditionFalse, Reason: "Idle", Message: "nothing to resolve", ObservedGeneration: rs.Generation})
	}
	r.setReady(st, rs.Generation, true, "Synced", "catalogue is current")
	if !plan.Empty() || st.LastSyncTime == nil || now.Sub(st.LastSyncTime.Time) >= lastSyncWriteEvery {
		st.LastSyncTime = &metav1.Time{Time: now}
	}
	if leftover > 0 {
		return r.finish(ctx, &rs, st, interval, syncingRequeue)
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

func (r *ReleaseSourceReconciler) skippedFor(rs *kuberikcomv1alpha1.ReleaseSource) map[string]struct{} {
	r.skipMu.Lock()
	defer r.skipMu.Unlock()
	return r.skipped[skipKey{rs.UID, rs.Generation}]
}

func (r *ReleaseSourceReconciler) markSkipped(rs *kuberikcomv1alpha1.ReleaseSource, tag string) {
	r.skipMu.Lock()
	defer r.skipMu.Unlock()
	if r.skipped == nil {
		r.skipped = map[skipKey]map[string]struct{}{}
	}
	k := skipKey{rs.UID, rs.Generation}
	if r.skipped[k] == nil {
		r.skipped[k] = map[string]struct{}{}
	}
	r.skipped[k][tag] = struct{}{}
}

func (r *ReleaseSourceReconciler) forgetSkipped(uid types.UID) {
	r.skipMu.Lock()
	defer r.skipMu.Unlock()
	for k := range r.skipped {
		if k.uid == uid {
			delete(r.skipped, k)
		}
	}
}

func (r *ReleaseSourceReconciler) listSlices(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource) ([]kuberikcomv1alpha1.ReleaseSlice, error) {
	var list kuberikcomv1alpha1.ReleaseSliceList
	if err := r.List(ctx, &list, client.InNamespace(rs.Namespace), client.MatchingLabels{kuberikcomv1alpha1.ReleaseSourceLabel: rs.Name}); err != nil {
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

// finish writes status only if it changed and schedules the next sync.
func (r *ReleaseSourceReconciler) finish(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource, st *kuberikcomv1alpha1.ReleaseSourceStatus, interval, requeue time.Duration) (ctrl.Result, error) {
	if !apiequality.Semantic.DeepEqual(rs.Status, *st) {
		rs.Status = *st
		if err := r.Status().Update(ctx, rs); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: syncingRequeue}, nil
			}
			return ctrl.Result{}, err
		}
	}
	if requeue > 0 {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	return ctrl.Result{RequeueAfter: jitter(interval)}, nil
}

// fail records bad config: Ready=False, no slice writes.
func (r *ReleaseSourceReconciler) fail(ctx context.Context, rs *kuberikcomv1alpha1.ReleaseSource, st *kuberikcomv1alpha1.ReleaseSourceStatus, now time.Time, interval time.Duration, _ string, reason, msg string) (ctrl.Result, error) {
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
		r.Recorder = mgr.GetEventRecorderFor("release-controller")
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
		For(&kuberikcomv1alpha1.ReleaseSource{}).
		Owns(&kuberikcomv1alpha1.ReleaseSlice{}, builder.WithPredicates(deleteOnly)).
		WithOptions(controller.Options{MaxConcurrentReconciles: conc}).
		Named("releasesource").
		Complete(r)
}
