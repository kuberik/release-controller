// Command parity checks the OCI release source against rollout-controller's
// current availableReleases (design/release-controller.md section 5, "Parity
// definition" and 3a rule 10). Input is a baseline `kubectl get rollouts,
// imagepolicies, imagerepositories -A -o json`; the registry is queried for
// real. The catalogue is built by the controller's own code: catalogue.Admit
// (incremental + backfill admission) driven by a simulated sync loop with the
// real resolve budget (50 per sync) until it completes, with slicer.Compute
// writing the slices and applying whole-slice retention after every sync.
//
//	go run ./hack/parity --baseline a.json,b.json [--kubeconfig-context ctx]
//
// Without --kubeconfig-context a secretRef cannot be read, so the repository is
// queried anonymously (and a note is printed). With it, the dockerconfigjson
// Secret is read via `kubectl --context ctx get secret`.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	imagev1 "github.com/fluxcd/image-reflector-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/catalogue"
	"github.com/kuberik/release-controller/internal/slicer"
	"github.com/kuberik/release-controller/internal/source"
	"github.com/kuberik/release-controller/internal/source/oci"
)

// Verdicts check reports for one ReleaseSource.
const (
	verdictPass = "PASS"
	verdictWarn = "WARN"
	verdictFail = "FAIL"
)

type todayRelease struct {
	Tag                string            `json:"tag"`
	Digest             *string           `json:"digest"`
	Version            *string           `json:"version"`
	Revision           *string           `json:"revision"`
	Created            *time.Time        `json:"created"`
	Requires           map[string]string `json:"requires"`
	MetadataUnresolved bool              `json:"metadataUnresolved"`
}

type rollout struct {
	Metadata metav1.ObjectMeta `json:"metadata"`
	Spec     struct {
		ReleasesImagePolicy struct {
			Name string `json:"name"`
		} `json:"releasesImagePolicy"`
		RetentionDays *int32 `json:"availableReleasesRetentionDays"`
		MinCount      *int32 `json:"availableReleasesMinCount"`
	} `json:"spec"`
	Status struct {
		Available []todayRelease `json:"availableReleases"`
		History   []struct {
			Version struct {
				Tag     string       `json:"tag"`
				Created *metav1.Time `json:"created,omitempty"`
			} `json:"version"`
		} `json:"history"`
	} `json:"status"`
}

type baseline struct {
	name     string
	rollouts []rollout
	objs     []client.Object
	policies map[string]*imagev1.ImagePolicy // ns/name
}

func load(path string) (*baseline, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	b := &baseline{name: path, policies: map[string]*imagev1.ImagePolicy{}}
	for _, it := range list.Items {
		var tm metav1.TypeMeta
		_ = json.Unmarshal(it, &tm)
		switch tm.Kind {
		case "Rollout":
			var r rollout
			if err := json.Unmarshal(it, &r); err != nil {
				return nil, err
			}
			b.rollouts = append(b.rollouts, r)
		case "ImagePolicy":
			var p imagev1.ImagePolicy
			if err := json.Unmarshal(it, &p); err != nil {
				return nil, err
			}
			p.ResourceVersion = ""
			b.policies[p.Namespace+"/"+p.Name] = &p
			b.objs = append(b.objs, &p)
		case "ImageRepository":
			var p imagev1.ImageRepository
			if err := json.Unmarshal(it, &p); err != nil {
				return nil, err
			}
			p.ResourceVersion = ""
			b.objs = append(b.objs, &p)
		}
	}
	return b, nil
}

// built is the from-scratch result for one ImagePolicy and retention.
type built struct {
	src      *oci.Source
	listed   map[string]bool           // every tag the listing returned
	rels     map[string]source.Release // every tag the walk resolved (raw: created zero if absent)
	rejected map[string]string
	ever     map[string]source.Release // every release ever admitted (even if retention dropped it later)
	final    []source.Release          // the catalogue after the last sync, sequence order
	syncs    int
	resolves int
	note     string
	err      error
}

var (
	ctxFlag = flag.String("kubeconfig-context", "", "kubectl context to read dockerconfigjson secrets from (optional)")
	only    = flag.String("only", "", "only rollouts whose ns/name contains this")
	verbose = flag.Bool("v", false, "list every extra tag")
)

func main() {
	bl := flag.String("baseline", "", "comma-separated baseline JSON files")
	flag.Parse()
	if *bl == "" {
		fmt.Fprintln(os.Stderr, "--baseline required")
		os.Exit(2)
	}
	now := time.Now()
	failed := 0
	for _, path := range strings.Split(*bl, ",") {
		b, err := load(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		failed += run(b, now)
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func run(b *baseline, now time.Time) int {
	fmt.Printf("\n=== %s: %d rollouts ===\n", b.name, len(b.rollouts))
	cats := map[string]*built{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i := range b.rollouts {
		r := &b.rollouts[i]
		key := catKey(r)
		mu.Lock()
		if _, ok := cats[key]; ok {
			mu.Unlock()
			continue
		}
		c := &built{}
		cats[key] = c
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			build(b, r, now, c)
		}()
	}
	wg.Wait()

	pass, warn, fail := 0, 0, 0
	for i := range b.rollouts {
		r := &b.rollouts[i]
		id := r.Metadata.Namespace + "/" + r.Metadata.Name
		if *only != "" && !strings.Contains(id, *only) {
			continue
		}
		c := cats[catKey(r)]
		switch check(id, r, c, b) {
		case verdictPass:
			pass++
		case verdictWarn:
			warn++
		default:
			fail++
		}
	}
	fmt.Printf("--- %s: %d PASS, %d WARN (retagged, or deployed tag served by the reader fallback), %d FAIL\n", b.name, pass, warn, fail)
	return fail
}

func retentionOf(r *rollout) (days, minCount int32) {
	days, minCount = 7, 30
	if r.Spec.RetentionDays != nil {
		days = *r.Spec.RetentionDays
	}
	if r.Spec.MinCount != nil {
		minCount = *r.Spec.MinCount
	}
	return
}

func catKey(r *rollout) string {
	d, m := retentionOf(r)
	return fmt.Sprintf("%s/%s/%d/%d", r.Metadata.Namespace, r.Spec.ReleasesImagePolicy.Name, d, m)
}

func build(b *baseline, r *rollout, now time.Time, c *built) {
	ns, polName := r.Metadata.Namespace, r.Spec.ReleasesImagePolicy.Name
	days, minCount := retentionOf(r)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pol := b.policies[ns+"/"+polName]
	if pol == nil {
		c.err = fmt.Errorf("ImagePolicy %s/%s not in baseline", ns, polName)
		return
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = oci.AddToScheme(scheme)
	objs := append([]client.Object{}, b.objs...)
	// secretRef handling
	for _, o := range b.objs {
		repo, ok := o.(*imagev1.ImageRepository)
		if !ok || repo.Namespace != ns || repo.Name != pol.Spec.ImageRepositoryRef.Name || repo.Spec.SecretRef == nil {
			continue
		}
		if *ctxFlag == "" {
			c.note = "secretRef " + repo.Spec.SecretRef.Name + " not read (no --kubeconfig-context): anonymous"
			cp := repo.DeepCopy()
			cp.Spec.SecretRef = nil
			for j, x := range objs {
				if x == o {
					objs[j] = cp
				}
			}
			continue
		}
		out, err := exec.Command("kubectl", "--context", *ctxFlag, "-n", ns, "get", "secret", repo.Spec.SecretRef.Name, "-o", "json").Output()
		if err != nil {
			c.err = fmt.Errorf("kubectl get secret: %w", err)
			return
		}
		var sec corev1.Secret
		if err := json.Unmarshal(out, &sec); err != nil {
			c.err = err
			return
		}
		sec.ResourceVersion = ""
		objs = append(objs, &sec)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	rs := &v1alpha1.ReleaseSource{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "parity"},
		Spec: v1alpha1.ReleaseSourceSpec{OCI: &v1alpha1.OCISource{
			ImagePolicyRef: &corev1.LocalObjectReference{Name: polName}}},
	}
	s, err := oci.Factory(ctx, cl, rs)
	if err != nil {
		c.err = err
		return
	}
	c.src = s.(*oci.Source)
	res, err := c.src.Discover(ctx, "")
	if err != nil {
		c.err = err
		return
	}
	c.rels = map[string]source.Release{}
	c.rejected = map[string]string{}
	c.listed = map[string]bool{}
	c.ever = map[string]source.Release{}
	for _, ref := range res.Refs {
		c.listed[ref.Tag] = true
	}
	resolve := func(ref source.Ref) catalogue.Resolved {
		var rel source.Release
		var err error
		for try := 0; try < 3; try++ { // a registry hiccup must not stall the simulated walk
			rel, err = c.src.Resolve(ctx, ref)
			if err != nil || !rel.MetadataUnresolved {
				break
			}
			if _, rejected := c.src.Rejection(ref.Tag); rejected {
				break
			}
		}
		c.rels[ref.Tag] = rel
		out := catalogue.Resolved{Release: rel}
		if err != nil {
			c.rejected[ref.Tag] = err.Error()
			out.Rejected = err.Error()
		} else if msg, ok := c.src.Rejection(ref.Tag); ok {
			c.rejected[ref.Tag] = msg
			out.Rejected = msg
		}
		return out
	}
	// The controller's loop: Admit with the real budget, slicer.Compute for the
	// writes and whole-slice retention, until the catalogue is complete.
	scfg := slicer.Config{Namespace: ns, Source: "parity", Now: now, RetentionDays: days, MinCount: int(minCount), MaxPerSlice: 100, MaxBytes: 256 * 1024}
	var slices []v1alpha1.ReleaseSlice
	st := catalogue.State{}
	var walk catalogue.WalkMemory // the reconciler's walk memory, so resolves reflect controller cost
	for c.syncs = 0; c.syncs < 500; c.syncs++ {
		minTag := ""
		if len(st.Catalogue) > 0 {
			minTag = st.Catalogue[0].Tag
		}
		hint := walk.After(minTag)
		ar := catalogue.Admit(st, res.Refs, catalogue.Config{Less: c.src.Less, Resolve: resolve, Budget: 50, Days: days, MinCount: int(minCount), Now: now, WalkAfter: hint})
		c.resolves += ar.Resolves
		for _, a := range ar.Admitted {
			c.ever[a.Tag] = a
		}
		plan := slicer.Compute(slicer.Input{Existing: slices, New: ar.Admitted}, scfg)
		slices, scfg.NextIndex = plan.Final, plan.NextIndex
		st = catalogue.State{Catalogue: slicer.Merge(slices), BackfillComplete: ar.BackfillComplete}
		newMin := ""
		if len(st.Catalogue) > 0 {
			newMin = st.Catalogue[0].Tag
		}
		walk = walk.Next(hint, ar, newMin)
		if ar.Leftover == 0 && ar.BackfillComplete {
			c.syncs++
			break
		}
		if ar.Waiting {
			c.err = fmt.Errorf("backfill stalled on a transient failure after %d syncs", c.syncs)
			return
		}
	}
	c.final = st.Catalogue
}

// retagged: the registry's tag was pushed again after Flux/rollout-controller
// recorded it (registry created is later than the recorded one). Today's list
// never refreshes a resolved entry, so it is stale by construction.
func retagged(t todayRelease, got source.Release) bool {
	return t.Created != nil && !got.CreatedObserved && got.Created.After(*t.Created)
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func check(id string, r *rollout, c *built, b *baseline) string {
	days, minCount := retentionOf(r)
	if c.err != nil {
		fmt.Printf("FAIL %s: cannot build catalogue: %v\n", id, c.err)
		return verdictFail
	}
	cat := c.final
	pos := map[string]int{}
	for i, a := range cat {
		pos[a.Tag] = i
	}
	deployed := ""
	if len(r.Status.History) > 0 {
		deployed = r.Status.History[0].Version.Tag
	}
	stub := func(tag string) source.Release { return source.Release{Tag: tag} }

	var problems, warns []string
	accepted := make([]string, 0, len(r.Status.Available))
	today := map[string]todayRelease{}
	var minC, maxC *time.Time
	lastPos := -1
	for _, t := range r.Status.Available {
		today[t.Tag] = t
		if t.Created != nil {
			if minC == nil || t.Created.Before(*minC) {
				v := *t.Created
				minC = &v
			}
			if maxC == nil || t.Created.After(*maxC) {
				v := *t.Created
				maxC = &v
			}
		}
	}
	// deployedLeft judges the deployed tag missing from the catalogue (§3a.9:
	// rollout-controller falls back to catalogue releases created after the
	// deployed history entry; acceptable only when that yields candidates).
	// It runs before the retag branch: a retagged deployed tag is still the
	// tag the reader must be able to move on from.
	deployedLeft := func(tag, why string) {
		var dc *time.Time
		if hc := r.Status.History[0].Version.Created; hc != nil {
			tt := hc.Time
			dc = &tt
		}
		newer := 0
		for _, cr := range cat {
			if dc != nil && !cr.CreatedObserved && cr.Created.After(*dc) {
				newer++
			}
		}
		if newer > 0 {
			warns = append(warns, fmt.Sprintf("%s: DEPLOYED tag left the catalogue (%s); reader lagging fallback (§3a.9) offers %d releases created after it", tag, why, newer))
		} else {
			problems = append(problems, fmt.Sprintf("rule1 %s: deployed tag not in the catalogue (%s) and the lagging fallback has no candidates", tag, why))
		}
	}
	// rule 1, 2, 3
	for _, t := range r.Status.Available {
		p, inCat := pos[t.Tag]
		raw, resolved := c.rels[t.Tag]
		_, wasAdmitted := c.ever[t.Tag]
		switch {
		case !c.listed[t.Tag]:
			accepted = append(accepted, fmt.Sprintf("%s: gone from registry or excluded by filter (today keeps deleted tags; unreproducible)", t.Tag))
			continue
		case !inCat && deployed != "" && t.Tag == deployed:
			why := "dropped by whole-slice retention"
			if !wasAdmitted && resolved {
				why = "excluded by the created rule"
			} else if !wasAdmitted {
				why = "not reached: older than the retention window"
			}
			deployedLeft(t.Tag, why)
			continue
		case !inCat && !wasAdmitted && resolved && retagged(t, raw):
			warns = append(warns, fmt.Sprintf("%s: RETAGGED since observed (today created %s, registry now %s, rev %s -> %s); the created rule then disagrees with the policy order so backfill excludes it", t.Tag, t.Created.Format(time.RFC3339), raw.Created.Format(time.RFC3339), str(t.Revision), str(raw.Revision)))
			continue
		case !inCat:
			why := "dropped by whole-slice retention"
			if !wasAdmitted && resolved {
				why = "excluded by the created rule"
			} else if !wasAdmitted {
				why = "not reached: older than the retention window"
			}
			if deployed != "" && c.src.Less(stub(t.Tag), stub(deployed)) {
				accepted = append(accepted, fmt.Sprintf("%s: history-keep (%s; older than the deployed %s; days=%d minCount=%d)", t.Tag, why, deployed, days, minCount))
			} else {
				problems = append(problems, fmt.Sprintf("rule1 %s: not in the catalogue (%s) and not older than the deployed tag %q", t.Tag, why, deployed))
			}
			continue
		}
		got := cat[p]
		// rule 2
		if d := diff(t, got); d != "" && retagged(t, got) {
			warns = append(warns, fmt.Sprintf("%s: RETAGGED since observed (today's entry is stale, never refreshed): %s", t.Tag, d))
		} else if d != "" {
			problems = append(problems, fmt.Sprintf("rule2 %s: %s", t.Tag, d))
		}
		// rule 3: the catalogue order is the sequence order
		if p < lastPos {
			problems = append(problems, fmt.Sprintf("rule3 %s: order differs from today's list", t.Tag))
		}
		lastPos = p
	}
	// rule 4
	reasons := map[string][]string{}
	created := r.Metadata.CreationTimestamp.Time
	var latest *source.Release
	if pol := b.policies[r.Metadata.Namespace+"/"+r.Spec.ReleasesImagePolicy.Name]; pol != nil && pol.Status.LatestRef != nil {
		if l, ok := c.ever[pol.Status.LatestRef.Tag]; ok {
			latest = &l
		}
	}
	for _, a := range cat {
		if _, ok := today[a.Tag]; ok {
			continue
		}
		var why string
		switch {
		case !a.CreatedObserved && a.Created.Time.Before(created):
			why = "predates-rollout"
		case latest != nil && c.src.Less(*latest, a):
			why = "pushed-after-baseline"
		case !a.CreatedObserved && minC != nil && a.Created.Time.Before(*minC):
			why = "predates-first-observed"
		case !a.CreatedObserved && minC != nil && maxC != nil && !a.Created.Time.Before(*minC) && !a.Created.After(*maxC):
			why = "pushed-between-scans"
		case a.CreatedObserved:
			why = "other: no created annotation (created=firstSeen)"
		default:
			why = "other"
			problems = append(problems, fmt.Sprintf("rule4 %s: extra tag with no accepted explanation (created %s)", a.Tag, a.Created.Format(time.RFC3339)))
		}
		reasons[why] = append(reasons[why], a.Tag)
	}
	for tag, why := range c.rejected {
		accepted = append(accepted, fmt.Sprintf("%s: MetadataRejected (%s)", tag, why))
	}

	verdict := verdictPass
	if len(warns) > 0 {
		verdict = verdictWarn
	}
	if len(problems) > 0 {
		verdict = verdictFail
	}
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rs := make([]string, 0, len(keys))
	for _, k := range keys {
		rs = append(rs, fmt.Sprintf("%s=%d", k, len(reasons[k])))
	}
	fmt.Printf("%s %s: today=%d catalogue=%d (admitted=%d listed=%d resolves=%d syncs=%d) deployed=%s extras{%s}\n",
		verdict, id, len(r.Status.Available), len(cat), len(c.ever), len(c.listed), c.resolves, c.syncs, deployed, strings.Join(rs, " "))
	if c.note != "" {
		fmt.Printf("     note: %s\n", c.note)
	}
	for _, p := range problems {
		fmt.Printf("     - %s\n", p)
	}
	for _, w := range warns {
		fmt.Printf("     ! %s\n", w)
	}
	for _, a := range accepted {
		fmt.Printf("     ~ %s\n", a)
	}
	if *verbose {
		for _, k := range keys {
			fmt.Printf("     extras %s: %s\n", k, strings.Join(reasons[k], ", "))
		}
	}
	return verdict
}

func diff(t todayRelease, got source.Release) string {
	var d []string
	if t.Digest != nil && (got.Digest == nil || *t.Digest != *got.Digest) {
		d = append(d, fmt.Sprintf("digest %s != %s", str(t.Digest), str(got.Digest)))
	}
	if str(t.Version) != str(got.Version) {
		d = append(d, fmt.Sprintf("version %s != %s", str(t.Version), str(got.Version)))
	}
	if str(t.Revision) != str(got.Revision) {
		d = append(d, fmt.Sprintf("revision %s != %s", str(t.Revision), str(got.Revision)))
	}
	switch {
	case t.Created == nil && !got.Created.IsZero() && !got.CreatedObserved:
		d = append(d, "created absent today, present now")
	case t.Created != nil && (got.CreatedObserved || !got.Created.Time.Equal(*t.Created)):
		d = append(d, fmt.Sprintf("created %s != %s", t.Created.Format(time.RFC3339), got.Created.Format(time.RFC3339)))
	}
	if len(t.Requires) != len(got.Requires) {
		d = append(d, fmt.Sprintf("requires %v != %v", t.Requires, got.Requires))
	} else {
		for k, v := range t.Requires {
			if gv, ok := got.Requires[k]; !ok || gv != v {
				d = append(d, fmt.Sprintf("requires[%s] %q != %q", k, v, gv))
			}
		}
	}
	if t.MetadataUnresolved != got.MetadataUnresolved {
		d = append(d, fmt.Sprintf("metadataUnresolved %v != %v", t.MetadataUnresolved, got.MetadataUnresolved))
	}
	return strings.Join(d, "; ")
}
