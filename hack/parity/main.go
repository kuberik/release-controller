// Command parity checks the OCI release source against rollout-controller's
// current availableReleases (design/release-controller.md section 5, "Parity
// definition"). Input is a baseline `kubectl get rollouts,imagepolicies,
// imagerepositories -A -o json`; the registry is queried for real.
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
	"github.com/kuberik/release-controller/internal/source"
	"github.com/kuberik/release-controller/internal/source/oci"
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

// catalogue is the from-scratch result for one ImagePolicy.
type catalogue struct {
	src      *oci.Source
	rels     map[string]source.Release // every discovered+resolved tag
	rejected map[string]string
	admitted []source.Release // monotonic, ascending
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
	cats := map[string]*catalogue{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i := range b.rollouts {
		r := &b.rollouts[i]
		key := r.Metadata.Namespace + "/" + r.Spec.ReleasesImagePolicy.Name
		mu.Lock()
		if _, ok := cats[key]; ok {
			mu.Unlock()
			continue
		}
		c := &catalogue{}
		cats[key] = c
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			build(b, r.Metadata.Namespace, r.Spec.ReleasesImagePolicy.Name, now, c)
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
		c := cats[r.Metadata.Namespace+"/"+r.Spec.ReleasesImagePolicy.Name]
		switch check(id, r, c, b, now) {
		case "PASS":
			pass++
		case "WARN":
			warn++
		default:
			fail++
		}
	}
	fmt.Printf("--- %s: %d PASS, %d WARN (retagged since observed), %d FAIL\n", b.name, pass, warn, fail)
	return fail
}

func build(b *baseline, ns, polName string, now time.Time, c *catalogue) {
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
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, ref := range res.Refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rel, err := c.src.Resolve(ctx, ref)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				c.rejected[ref.Tag] = err.Error()
			}
			c.rels[ref.Tag] = rel
		}()
	}
	wg.Wait()

	// created: source value, else firstSeen (= now on a from-scratch build).
	all := make([]source.Release, 0, len(c.rels))
	for _, r := range c.rels {
		if r.Created.IsZero() {
			r.Created = metav1.NewTime(now)
			r.CreatedObserved = true
		}
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].Created.Equal(&all[j].Created) {
			return all[i].Created.Before(&all[j].Created)
		}
		return all[i].Tag < all[j].Tag
	})
	if c.src.AdmitAll() {
		c.admitted = all
		return
	}
	var max *source.Release
	for i := range all {
		if max == nil || c.src.Less(*max, all[i]) {
			c.admitted = append(c.admitted, all[i])
			max = &all[i]
		}
	}
}

func retain(rels []source.Release, days, minCount int32, now time.Time) map[string]bool {
	keep := map[string]bool{}
	for i := len(rels) - 1; i >= 0; i-- { // newest first
		n := len(rels) - i
		r := rels[i]
		young := days == 0 || !r.Created.Time.Before(now.AddDate(0, 0, -int(days)))
		if n <= int(minCount) || young {
			keep[r.Tag] = true
		}
	}
	return keep
}

// blocker explains why tag was not admitted: the running max at its created
// time that it does not exceed under the policy.
func blocker(c *catalogue, rel source.Release) string {
	var max *source.Release
	for i := range c.admitted {
		if c.admitted[i].Created.Time.After(rel.Created.Time) || (c.admitted[i].Created.Equal(&rel.Created) && c.admitted[i].Tag > rel.Tag) {
			break
		}
		max = &c.admitted[i]
	}
	if max == nil {
		return fmt.Sprintf("created %s", rel.Created.Format(time.RFC3339))
	}
	return fmt.Sprintf("created %s, rev %s; running max then: %s created %s, rev %s",
		rel.Created.Format(time.RFC3339), str(rel.Revision), max.Tag, max.Created.Format(time.RFC3339), str(max.Revision))
}

// retagged: the registry's tag was pushed again after Flux/rollout-controller
// recorded it (registry created is later than the recorded one). Today's list
// never refreshes a resolved entry, so it is stale by construction.
func retagged(t todayRelease, got source.Release) bool {
	return t.Created != nil && !got.CreatedObserved && got.Created.Time.After(*t.Created)
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func check(id string, r *rollout, c *catalogue, b *baseline, now time.Time) string {
	days, minCount := int32(7), int32(30)
	if r.Spec.RetentionDays != nil {
		days = *r.Spec.RetentionDays
	}
	if r.Spec.MinCount != nil {
		minCount = *r.Spec.MinCount
	}
	if c.err != nil {
		fmt.Printf("FAIL %s: cannot build catalogue: %v\n", id, c.err)
		return "FAIL"
	}
	keep := retain(c.admitted, days, minCount, now)
	var cat []source.Release
	pos := map[string]int{}
	for _, a := range c.admitted {
		if keep[a.Tag] {
			pos[a.Tag] = len(cat)
			cat = append(cat, a)
		}
	}
	admitted := map[string]bool{}
	for _, a := range c.admitted {
		admitted[a.Tag] = true
	}

	var problems, accepted, warns []string
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
	// rule 1, 2, 3
	for _, t := range r.Status.Available {
		got, discovered := c.rels[t.Tag]
		p, inCat := pos[t.Tag]
		switch {
		case !discovered:
			accepted = append(accepted, fmt.Sprintf("%s: gone from registry or excluded by filter (today keeps deleted tags; unreproducible)", t.Tag))
			continue
		case !admitted[t.Tag] && retagged(t, got):
			warns = append(warns, fmt.Sprintf("%s: RETAGGED since observed (today created %s, registry now %s, rev %s -> %s); created order then disagrees with the policy so monotonic admission excludes it", t.Tag, t.Created.Format(time.RFC3339), got.Created.Format(time.RFC3339), str(t.Revision), str(got.Revision)))
			continue
		case !admitted[t.Tag]:
			problems = append(problems, fmt.Sprintf("rule1 %s: discovered but not admitted by monotonic rule (%s)", t.Tag, blocker(c, got)))
			continue
		case !inCat:
			accepted = append(accepted, fmt.Sprintf("%s: admitted but dropped by retention (days=%d minCount=%d; accepted difference: history-keep no longer applies)", t.Tag, days, minCount))
			continue
		}
		// rule 2
		if d := diff(t, got); d != "" && retagged(t, got) {
			warns = append(warns, fmt.Sprintf("%s: RETAGGED since observed (today's entry is stale, never refreshed): %s", t.Tag, d))
		} else if d != "" {
			problems = append(problems, fmt.Sprintf("rule2 %s: %s", t.Tag, d))
		}
		// rule 3
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
		if l, ok := c.rels[pol.Status.LatestRef.Tag]; ok {
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
		case !a.CreatedObserved && minC != nil && maxC != nil && !a.Created.Time.Before(*minC) && !a.Created.Time.After(*maxC):
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

	verdict := "PASS"
	if len(warns) > 0 {
		verdict = "WARN"
	}
	if len(problems) > 0 {
		verdict = "FAIL"
	}
	var rs []string
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		rs = append(rs, fmt.Sprintf("%s=%d", k, len(reasons[k])))
	}
	fmt.Printf("%s %s: today=%d catalogue=%d (admitted=%d discovered=%d) extras{%s}\n",
		verdict, id, len(r.Status.Available), len(cat), len(c.admitted), len(c.rels), strings.Join(rs, " "))
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
