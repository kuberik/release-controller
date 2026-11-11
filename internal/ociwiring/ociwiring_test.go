// Package ociwiring wires the real OCI source into the real reconciler and
// manager against an in-memory registry and envtest, end to end.
package ociwiring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	imagev1 "github.com/fluxcd/image-reflector-controller/api/v1"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/google/go-containerregistry/pkg/registry"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/controller"
	"github.com/kuberik/release-controller/internal/source/oci"
)

const mtManifest = "application/vnd.oci.image.manifest.v1+json"

// countingReader records the Go type of every object read through it.
type countingReader struct {
	client.Reader
	gets *sync.Map
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	n, _ := r.gets.LoadOrStore(fmt.Sprintf("%T", obj), new(int32))
	atomic.AddInt32(n.(*int32), 1)
	return r.Reader.Get(ctx, key, obj, opts...)
}

type env struct {
	// manifestReqs counts registry requests for manifests (HEAD and GET), per tag.
	manifestReqs *sync.Map
	gets         *sync.Map
	c            client.Client
	host         string
	put          func(repo, tag string, ann map[string]string)
}

func binaryDir() string {
	base := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}

// start boots envtest (with the Flux image CRDs only when flux is true), a
// real manager and the real reconciler (nil SourceFactory: the registered OCI
// factory), and an in-memory registry.
func start(t *testing.T, flux bool, budget int) *env {
	t.Helper()
	gets := &sync.Map{}
	crds := []string{filepath.Join("..", "..", "config", "crd", "bases")}
	if flux {
		crds = append(crds, filepath.Join("..", "..", "testdata", "flux-crds"))
	}
	te := &envtest.Environment{CRDDirectoryPaths: crds, ErrorIfCRDPathMissing: true, BinaryAssetsDirectory: binaryDir()}
	cfg, err := te.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = te.Stop() })

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, oci.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controller.ReleaseSourceReconciler{
		Client: mgr.GetClient(), APIReader: &countingReader{Reader: mgr.GetAPIReader(), gets: gets}, Scheme: scheme,
		MaxReleasesPerSlice: 5, MaxSliceBytes: 256 * 1024, MaxResolvesPerSync: budget, MaxConcurrentReconciles: 2,
	}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	mctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
		}
	})

	manifestReqs := &sync.Map{}
	reg := registry.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if (req.Method == http.MethodGet || req.Method == http.MethodHead) && strings.Contains(req.URL.Path, "/manifests/") {
			n, _ := manifestReqs.LoadOrStore(req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:], new(int32))
			atomic.AddInt32(n.(*int32), 1)
		}
		reg.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{manifestReqs: manifestReqs, gets: gets, c: c, host: strings.TrimPrefix(srv.URL, "http://")}
	e.put = func(repo, tag string, ann map[string]string) {
		cfgDesc := map[string]any{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + strings.Repeat("a", 64), "size": 2}
		man := map[string]any{"schemaVersion": 2, "mediaType": mtManifest, "config": cfgDesc, "layers": []any{}}
		if ann != nil {
			man["annotations"] = ann
		}
		raw, _ := json.Marshal(man)
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v2/"+repo+"/manifests/"+tag, bytes.NewReader(raw))
		req.Header.Set("Content-Type", mtManifest)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("put %s:%s: %s", repo, tag, resp.Status)
		}
	}
	return e
}

func created(minsAgo int) map[string]string {
	return map[string]string{
		"org.opencontainers.image.created": time.Now().Add(-time.Duration(minsAgo) * time.Minute).UTC().Format(time.RFC3339),
		"org.opencontainers.image.title":   "Demo",
		"org.opencontainers.image.source":  "https://example.com/demo",
	}
}

func (e *env) slices(g Gomega, name string) []v1alpha1.ReleaseSlice {
	var l v1alpha1.ReleaseSliceList
	g.Expect(e.c.List(context.Background(), &l, client.InNamespace("default"), client.MatchingLabels{v1alpha1.ReleaseSourceLabel: name})).To(Succeed())
	sort.Slice(l.Items, func(i, j int) bool { return l.Items[i].Name < l.Items[j].Name })
	return l.Items
}

func tags(s v1alpha1.ReleaseSlice) []string {
	out := make([]string, 0, len(s.Releases))
	for _, r := range s.Releases {
		out = append(out, r.Tag)
	}
	return out
}

// kick makes the reconciler sync again now (any update re-enqueues).
func (e *env) kick(t *testing.T, name string) {
	t.Helper()
	g := NewWithT(t)
	g.Eventually(func() error {
		var rs v1alpha1.ReleaseSource
		if err := e.c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, &rs); err != nil {
			return err
		}
		if rs.Annotations == nil {
			rs.Annotations = map[string]string{}
		}
		rs.Annotations["kick"] = fmt.Sprint(time.Now().UnixNano())
		return e.c.Update(context.Background(), &rs)
	}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
}

func (e *env) events(g Gomega, name, reason string) []corev1.Event {
	var l corev1.EventList
	g.Expect(e.c.List(context.Background(), &l, client.InNamespace("default"))).To(Succeed())
	var out []corev1.Event
	for _, ev := range l.Items {
		if ev.InvolvedObject.Name == name && ev.Reason == reason {
			out = append(out, ev)
		}
	}
	return out
}

func TestOCIEndToEnd(t *testing.T) {
	g := NewWithT(t)
	e := start(t, false, 1000) // no Flux CRDs: the manager must start without informers for them
	ctx := context.Background()
	const name = "app"
	for i := 1; i <= 7; i++ {
		e.put("app", fmt.Sprintf("v%03d", i), created(100-i))
	}
	e.put("app", "latest", created(1)) // does not match filterTags
	rs := &v1alpha1.ReleaseSource{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.ReleaseSourceSpec{
			Interval: &metav1.Duration{Duration: time.Hour},
			OCI: &v1alpha1.OCISource{
				Image: e.host + "/app", Insecure: true,
				FilterTags: &v1alpha1.TagFilter{Pattern: `^v[0-9]+$`},
				Policy:     &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: "asc"}},
			},
		},
	}
	g.Expect(e.c.Create(ctx, rs)).To(Succeed())
	// A source pointing at a Flux ImagePolicy while the Flux CRDs are absent:
	// the uncached read fails fast with ConfigInvalid, no informer is started
	// and the other source is unaffected.
	noflux := &v1alpha1.ReleaseSource{
		ObjectMeta: metav1.ObjectMeta{Name: "noflux", Namespace: "default"},
		Spec: v1alpha1.ReleaseSourceSpec{
			Interval: &metav1.Duration{Duration: time.Hour},
			OCI:      &v1alpha1.OCISource{ImagePolicyRef: &corev1.LocalObjectReference{Name: "pol"}},
		},
	}
	g.Expect(e.c.Create(ctx, noflux)).To(Succeed())
	g.Eventually(func(g Gomega) {
		var got v1alpha1.ReleaseSource
		g.Expect(e.c.Get(ctx, client.ObjectKeyFromObject(noflux), &got)).To(Succeed())
		var ready *metav1.Condition
		for i := range got.Status.Conditions {
			if got.Status.Conditions[i].Type == "Ready" {
				ready = &got.Status.Conditions[i]
			}
		}
		g.Expect(ready).NotTo(BeNil())
		g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(ready.Reason).To(Equal("ConfigInvalid"))
	}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

	By := func(s string) { t.Log(s) }
	By("7 tags -> backfill walks newest first: slice 0 = v003..v007, the open slice 1 = v001, v002; artifact filled")
	g.Eventually(func(g Gomega) {
		s := e.slices(g, name)
		g.Expect(s).To(HaveLen(2))
		g.Expect(tags(s[0])).To(Equal([]string{"v003", "v004", "v005", "v006", "v007"}))
		g.Expect(s[0].Sealed).To(BeTrue())
		g.Expect(tags(s[1])).To(Equal([]string{"v001", "v002"}))
		g.Expect(s[1].Sealed).To(BeFalse())
		var got v1alpha1.ReleaseSource
		g.Expect(e.c.Get(ctx, client.ObjectKeyFromObject(rs), &got)).To(Succeed())
		g.Expect(got.Status.Count).To(Equal(int32(7)))
		g.Expect(got.Status.SliceCount).To(Equal(int32(2)))
		g.Expect(got.Status.OpenSlice).To(Equal(s[1].Name))
		g.Expect(got.Status.BackfillComplete).To(BeTrue())
		g.Expect(got.Status.Newest).NotTo(BeNil())
		g.Expect(got.Status.Newest.Tag).To(Equal("v007"))
		g.Expect(got.Status.Artifact).NotTo(BeNil())
		g.Expect(got.Status.Artifact.Title).To(Equal("Demo"))
		g.Expect(got.Status.Artifact.SourceURL).To(Equal("https://example.com/demo"))
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())
	for _, s := range e.slices(g, name) {
		for _, r := range s.Releases {
			g.Expect(r.MetadataUnresolved).To(BeFalse(), r.Tag)
			g.Expect(r.CreatedObserved).To(BeFalse(), r.Tag)
			g.Expect(r.Digest).NotTo(BeNil())
		}
	}
	before := e.slices(g, name)

	By("one more tag -> only the open slice changes")
	e.put("app", "v008", created(10))
	e.kick(t, name)
	g.Eventually(func(g Gomega) {
		s := e.slices(g, name)
		g.Expect(s).To(HaveLen(2))
		g.Expect(tags(s[1])).To(Equal([]string{"v001", "v002", "v008"})) // the open slice takes new releases
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())
	after := e.slices(g, name)
	g.Expect(after[0].ResourceVersion).To(Equal(before[0].ResourceVersion), "sealed slice must not be written")
	g.Expect(after[1].ResourceVersion).NotTo(Equal(before[1].ResourceVersion))

	By("backport tag (lower under the policy) is not admitted")
	e.put("app", "v000", created(500))
	e.kick(t, name)
	g.Consistently(func(g Gomega) {
		for _, s := range e.slices(g, name) {
			g.Expect(tags(s)).NotTo(ContainElement("v000"))
		}
	}, 3*time.Second, 500*time.Millisecond).Should(Succeed())

	By("image without created annotation -> createdObserved")
	e.put("app", "v009", map[string]string{"org.opencontainers.image.title": "Demo"})
	e.kick(t, name)
	g.Eventually(func(g Gomega) {
		s := e.slices(g, name)
		var found *v1alpha1.Release
		for _, sl := range s {
			for i := range sl.Releases {
				if sl.Releases[i].Tag == "v009" {
					found = &sl.Releases[i]
				}
			}
		}
		g.Expect(found).NotTo(BeNil())
		g.Expect(found.CreatedObserved).To(BeTrue())
		g.Expect(found.MetadataUnresolved).To(BeFalse())
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())

	By("over-bound requires -> MetadataUnresolved + MetadataRejected Event")
	e.put("app", "v010", map[string]string{"com.kuberik.rollout.requires." + strings.Repeat("k", 65): "1"})
	e.kick(t, name)
	g.Eventually(func(g Gomega) {
		var found *v1alpha1.Release
		for _, sl := range e.slices(g, name) {
			for i := range sl.Releases {
				if sl.Releases[i].Tag == "v010" {
					found = &sl.Releases[i]
				}
			}
		}
		g.Expect(found).NotTo(BeNil())
		g.Expect(found.MetadataUnresolved).To(BeTrue())
		g.Expect(found.Requires).To(BeEmpty())
		evs := e.events(g, name, "MetadataRejected")
		g.Expect(evs).NotTo(BeEmpty())
		g.Expect(evs[0].Type).To(Equal(corev1.EventTypeWarning))
		g.Expect(evs[0].Message).To(ContainSubstring("v010"))
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())

	By("over-long version -> rejected, slice write still succeeds")
	e.put("app", "v011", map[string]string{"org.opencontainers.image.version": strings.Repeat("9", 200)})
	e.kick(t, name)
	g.Eventually(func(g Gomega) {
		var found *v1alpha1.Release
		for _, sl := range e.slices(g, name) {
			for i := range sl.Releases {
				if sl.Releases[i].Tag == "v011" {
					found = &sl.Releases[i]
				}
			}
		}
		g.Expect(found).NotTo(BeNil())
		g.Expect(found.MetadataUnresolved).To(BeTrue())
		g.Expect(found.Version).To(BeNil())
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())
}

func TestOCIImagePolicyRef(t *testing.T) {
	g := NewWithT(t)
	e := start(t, true, 1000)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		e.put("pol", fmt.Sprintf("rel-%d", i), created(50-i))
	}
	e.put("pol", "junk", created(1))
	g.Expect(e.c.Create(ctx, &imagev1.ImageRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "default"},
		Spec:       imagev1.ImageRepositorySpec{Image: e.host + "/pol", Insecure: true, Interval: metav1.Duration{Duration: time.Hour}},
	})).To(Succeed())
	g.Expect(e.c.Create(ctx, &imagev1.ImagePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default"},
		Spec: imagev1.ImagePolicySpec{
			ImageRepositoryRef: meta.NamespacedObjectReference{Name: "repo"},
			FilterTags:         &imagev1.TagFilter{Pattern: `^rel-[0-9]+$`},
			Policy:             imagev1.ImagePolicyChoice{Alphabetical: &imagev1.AlphabeticalPolicy{Order: "asc"}},
		},
	})).To(Succeed())
	rs := &v1alpha1.ReleaseSource{
		ObjectMeta: metav1.ObjectMeta{Name: "viaref", Namespace: "default"},
		Spec: v1alpha1.ReleaseSourceSpec{
			Interval: &metav1.Duration{Duration: time.Hour},
			OCI:      &v1alpha1.OCISource{ImagePolicyRef: &corev1.LocalObjectReference{Name: "pol"}},
		},
	}
	g.Expect(e.c.Create(ctx, rs)).To(Succeed())
	g.Eventually(func(g Gomega) {
		s := e.slices(g, "viaref")
		g.Expect(s).To(HaveLen(1))
		g.Expect(tags(s[0])).To(Equal([]string{"rel-1", "rel-2", "rel-3"}))
		var got v1alpha1.ReleaseSource
		g.Expect(e.c.Get(ctx, client.ObjectKeyFromObject(rs), &got)).To(Succeed())
		g.Expect(got.Status.Newest).NotTo(BeNil())
		g.Expect(got.Status.Newest.Tag).To(Equal("rel-3"))
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())
	// ImagePolicy and ImageRepository were read through the uncached reader.
	for _, typ := range []string{"*v1.ImagePolicy", "*v1.ImageRepository"} {
		n, ok := e.gets.Load(typ)
		g.Expect(ok).To(BeTrue(), typ)
		g.Expect(atomic.LoadInt32(n.(*int32))).To(BeNumerically(">", 0), typ)
	}
}

// 120 tags, resolve budget 50, a policy: every tag inside the retention window
// is admitted across syncs, sequences follow the policy strictly, tags older
// than the window are never resolved, and a restarted reconciler costs no
// manifest requests at all (tags below the maximum are never touched).
func TestOCIBudgetBackfillAndRestart(t *testing.T) {
	g := NewWithT(t)
	e := start(t, false, 50)
	ctx := context.Background()
	const name = "big"
	for i := 1; i <= 120; i++ {
		ann := created(200 - i) // minutes ago: inside the 7 day window
		if i <= 40 {
			ann["org.opencontainers.image.created"] = time.Now().Add(-20 * 24 * time.Hour).Add(time.Duration(i) * time.Minute).UTC().Format(time.RFC3339)
		}
		e.put("big", fmt.Sprintf("v%03d", i), ann)
	}
	rs := &v1alpha1.ReleaseSource{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.ReleaseSourceSpec{
			Interval: &metav1.Duration{Duration: time.Hour},
			OCI: &v1alpha1.OCISource{
				Image: e.host + "/big", Insecure: true,
				FilterTags: &v1alpha1.TagFilter{Pattern: `^v[0-9]+$`},
				Policy:     &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: "asc"}},
			},
		},
	}
	g.Expect(e.c.Create(ctx, rs)).To(Succeed())

	// v120..v041 are in the window; v040 is the first past it (and minCount 30
	// is met), where the walk stops: 81 releases.
	g.Eventually(func(g Gomega) {
		var got v1alpha1.ReleaseSource
		g.Expect(e.c.Get(ctx, client.ObjectKeyFromObject(rs), &got)).To(Succeed())
		g.Expect(got.Status.BackfillComplete).To(BeTrue())
		g.Expect(got.Status.Count).To(Equal(int32(81)))
	}, 90*time.Second, 250*time.Millisecond).Should(Succeed())

	var all []v1alpha1.Release
	for _, s := range e.slices(g, name) {
		all = append(all, s.Releases...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Sequence < all[j].Sequence })
	g.Expect(all).To(HaveLen(81))
	for i, r := range all {
		g.Expect(r.Tag).To(Equal(fmt.Sprintf("v%03d", 40+i)), "catalogue order must be policy order")
		g.Expect(r.MetadataUnresolved).To(BeFalse())
		if i > 0 {
			g.Expect(r.Sequence).To(BeNumerically(">", all[i-1].Sequence))
		}
	}
	for i := 1; i < 40; i++ { // never resolved: below the stop
		_, requested := e.manifestReqs.Load(fmt.Sprintf("v%03d", i))
		g.Expect(requested).To(BeFalse(), fmt.Sprintf("v%03d is older than the window and must not be resolved", i))
	}

	// a new tag arrives: admitted incrementally above the maximum
	e.put("big", "v121", created(1))
	e.kick(t, name)
	g.Eventually(func(g Gomega) {
		var got v1alpha1.ReleaseSource
		g.Expect(e.c.Get(ctx, client.ObjectKeyFromObject(rs), &got)).To(Succeed())
		g.Expect(got.Status.Count).To(Equal(int32(82)))
		g.Expect(got.Status.Newest.Tag).To(Equal("v121"))
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed())

	// restart: a fresh reconciler instance (no memory) syncs the same source.
	reqs := func() int32 {
		var n int32
		e.manifestReqs.Range(func(_, v any) bool { n += atomic.LoadInt32(v.(*int32)); return true })
		return n
	}
	before := reqs()
	fresh := &controller.ReleaseSourceReconciler{Client: e.c, APIReader: e.c, Scheme: e.c.Scheme(),
		MaxReleasesPerSlice: 5, MaxSliceBytes: 256 * 1024, MaxResolvesPerSync: 50}
	_, err := fresh.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(rs)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(reqs()-before).To(BeZero(), "a restart must not fetch any manifest")
	var got v1alpha1.ReleaseSource
	g.Expect(e.c.Get(ctx, client.ObjectKeyFromObject(rs), &got)).To(Succeed())
	g.Expect(got.Status.Count).To(Equal(int32(82)))
	t.Logf("registry manifest requests so far: %d (81+1 tags resolved, HEAD+GET each)", reqs())
}
