package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	imagev1 "github.com/fluxcd/image-reflector-controller/api/v1"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/google/go-containerregistry/pkg/registry"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/source"
)

const (
	mtManifest = "application/vnd.oci.image.manifest.v1+json"
	mtIndex    = "application/vnd.oci.image.index.v1+json"
)

type reg struct {
	t    *testing.T
	srv  *httptest.Server
	host string
}

func newReg(t *testing.T, wrap func(http.Handler) http.Handler) *reg {
	h := http.Handler(registry.New())
	if wrap != nil {
		h = wrap(h)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &reg{t: t, srv: srv, host: strings.TrimPrefix(srv.URL, "http://")}
}

// put pushes a raw manifest and returns its digest.
func (r *reg) put(repo, ref, mediaType string, body any) string {
	r.t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/v2/"+repo+"/manifests/"+ref, bytes.NewReader(raw))
	req.Header.Set("Content-Type", mediaType)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("u:p")))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		r.t.Fatalf("put %s:%s: %s", repo, ref, resp.Status)
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

type m map[string]any

func image(manifestAnn, configAnn map[string]string, extra m) m {
	cfg := m{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + strings.Repeat("a", 64), "size": 2}
	if configAnn != nil {
		cfg["annotations"] = configAnn
	}
	out := m{"schemaVersion": 2, "mediaType": mtManifest, "config": cfg, "layers": []any{}}
	if manifestAnn != nil {
		out["annotations"] = manifestAnn
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func newSrc(t *testing.T, r *reg, repo string, f *v1alpha1.TagFilter, p *v1alpha1.OrderPolicy) *Source {
	t.Helper()
	s, err := New(Config{Image: r.host + "/" + repo, Insecure: true, FilterTags: f, Policy: p})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func resolve(t *testing.T, s *Source, tag string) source.Release {
	t.Helper()
	rel, err := s.Resolve(context.Background(), source.Ref{Tag: tag})
	if err != nil {
		t.Fatalf("resolve %s: %v", tag, err)
	}
	return rel
}

func tagsOf(res source.DiscoverResult) []string {
	var out []string
	for _, r := range res.Refs {
		out = append(out, r.Tag)
	}
	return out
}

func TestDiscoverFilterAndExtract(t *testing.T) {
	r := newReg(t, nil)
	for _, tag := range []string{"main-100-aaa", "main-200-bbb", "pr-1", "latest"} {
		r.put("app", tag, mtManifest, image(nil, nil, nil))
	}
	s := newSrc(t, r, "app",
		&v1alpha1.TagFilter{Pattern: `^main-(?P<ts>[0-9]+)-[a-f0-9]+`, Extract: "$ts"},
		&v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: "asc"}})
	res, err := s.Discover(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	got := tagsOf(res)
	if len(got) != 2 || res.NotModified || res.Cursor != "" {
		t.Fatalf("got %v", got)
	}
	a, b := source.Release{Tag: "main-100-aaa"}, source.Release{Tag: "main-200-bbb"}
	if !s.Less(a, b) || s.Less(b, a) {
		t.Fatal("extracted ordering wrong")
	}
	if s.AdmitAll() {
		t.Fatal("policy set, AdmitAll must be false")
	}
}

func TestDiscoverPaginates(t *testing.T) {
	// The in-memory registry does not emit Link headers, so serve the tag list
	// ourselves in pages of 10 with RFC 5988 Link: rel="next".
	var all []string
	for i := 0; i < 25; i++ {
		all = append(all, fmt.Sprintf("t%02d", i))
	}
	pages := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v2/" {
			return
		}
		pages++
		last := req.URL.Query().Get("last")
		var page []string
		for _, tg := range all {
			if tg > last && len(page) < 10 {
				page = append(page, tg)
			}
		}
		if len(page) == 10 && page[9] != all[len(all)-1] {
			w.Header().Set("Link", fmt.Sprintf(`</v2/app/tags/list?last=%s&n=10>; rel="next"`, page[9]))
		}
		json.NewEncoder(w).Encode(m{"name": "app", "tags": page})
	}))
	defer srv.Close()
	s, _ := New(Config{Image: strings.TrimPrefix(srv.URL, "http://") + "/app", Insecure: true})
	res, err := s.Discover(context.Background(), "")
	if err != nil || len(res.Refs) != 25 {
		t.Fatalf("err=%v n=%d", err, len(res.Refs))
	}
	if pages != 3 {
		t.Fatalf("expected 3 tag-list pages, got %d", pages)
	}
	if !s.AdmitAll() {
		t.Fatal("no policy => AdmitAll")
	}
}

func TestSemverRangeFilters(t *testing.T) {
	r := newReg(t, nil)
	for _, tag := range []string{"1.2.0", "1.2.5", "1.3.0", "2.0.0", "v1.9.0", "latest", "1.2"} {
		r.put("app", tag, mtManifest, image(nil, nil, nil))
	}
	s := newSrc(t, r, "app", nil, &v1alpha1.OrderPolicy{SemVer: &v1alpha1.SemVerPolicy{Range: ">=1.2.0 <2.0.0"}})
	res, _ := s.Discover(context.Background(), "")
	got := map[string]bool{}
	for _, x := range tagsOf(res) {
		got[x] = true
	}
	for _, want := range []string{"1.2.0", "1.2.5", "1.3.0", "v1.9.0"} {
		if !got[want] {
			t.Errorf("missing %s in %v", want, got)
		}
	}
	for _, no := range []string{"2.0.0", "latest", "1.2"} {
		if got[no] {
			t.Errorf("%s must be filtered out", no)
		}
	}
	if !s.Less(source.Release{Tag: "1.2.5"}, source.Release{Tag: "1.3.0"}) || !s.Less(source.Release{Tag: "1.3.0"}, source.Release{Tag: "v1.9.0"}) {
		t.Fatal("semver order")
	}
	if s.Less(source.Release{Tag: "1.3.0"}, source.Release{Tag: "1.2.5"}) {
		t.Fatal("semver order reversed")
	}
}

func TestPolicyLess(t *testing.T) {
	rel := func(t string) source.Release { return source.Release{Tag: t} }
	cases := []struct {
		name string
		pol  *v1alpha1.OrderPolicy
		a, b string
		want bool // Less(a,b)
	}{
		{"alpha asc", &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: "asc"}}, "a", "b", true},
		{"alpha default asc", &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{}}, "a", "b", true},
		{"alpha desc", &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: "desc"}}, "a", "b", false},
		{"alpha desc rev", &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: "desc"}}, "b", "a", true},
		{"num asc", &v1alpha1.OrderPolicy{Numerical: &v1alpha1.NumericalPolicy{Order: "asc"}}, "9", "10", true},
		{"num desc", &v1alpha1.OrderPolicy{Numerical: &v1alpha1.NumericalPolicy{Order: "desc"}}, "9", "10", false},
		{"alpha is not numeric", &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{}}, "9", "10", false},
		{"semver", &v1alpha1.OrderPolicy{SemVer: &v1alpha1.SemVerPolicy{Range: "*"}}, "1.9.0", "1.10.0", true},
	}
	for _, c := range cases {
		s, err := New(Config{Image: "x/y", Policy: c.pol})
		if err != nil {
			t.Fatal(c.name, err)
		}
		if got := s.Less(rel(c.a), rel(c.b)); got != c.want {
			t.Errorf("%s: Less(%s,%s)=%v want %v", c.name, c.a, c.b, got, c.want)
		}
	}
	// numerical: rel-<n> extracted
	s, _ := New(Config{Image: "x/y",
		FilterTags: &v1alpha1.TagFilter{Pattern: `^rel-(?P<n>[0-9]+)$`, Extract: "$n"},
		Policy:     &v1alpha1.OrderPolicy{Numerical: &v1alpha1.NumericalPolicy{Order: "asc"}}})
	if !s.Less(rel("rel-9"), rel("rel-10")) {
		t.Fatal("numerical on extracted value")
	}
	// no policy: by created then tag
	s, _ = New(Config{Image: "x/y"})
	now := metav1.Now()
	earlier := metav1.NewTime(now.Add(-time.Hour))
	if !s.Less(source.Release{Tag: "z", Created: earlier}, source.Release{Tag: "a", Created: now}) {
		t.Fatal("no policy: by created")
	}
	if _, err := New(Config{Image: "x/y", Policy: &v1alpha1.OrderPolicy{SemVer: &v1alpha1.SemVerPolicy{Range: "not a range"}}}); err == nil {
		t.Fatal("bad range must error")
	}
}

func TestResolveAnnotationPrecedence(t *testing.T) {
	r := newReg(t, nil)
	created := "2026-11-01T10:00:00Z"
	// manifest wins over config per field; config fills gaps; empty ignored.
	d := r.put("app", "full", mtManifest, image(
		map[string]string{
			"org.opencontainers.image.version":  "1.0.0",
			"org.opencontainers.image.revision": "", // empty: ignored -> config fallback
			"org.opencontainers.image.created":  "not a time",
		},
		map[string]string{
			"org.opencontainers.image.version":  "9.9.9",
			"org.opencontainers.image.revision": "cfgsha",
			"org.opencontainers.image.created":  created,
		}, nil))
	s := newSrc(t, r, "app", nil, nil)
	rel := resolve(t, s, "full")
	if rel.MetadataUnresolved || rel.Digest == nil || *rel.Digest != d {
		t.Fatalf("%+v want digest %s", rel, d)
	}
	if *rel.Version != "1.0.0" || *rel.Revision != "cfgsha" {
		t.Fatalf("precedence: v=%s r=%s", *rel.Version, *rel.Revision)
	}
	if want, _ := time.Parse(time.RFC3339, created); !rel.Created.Time.Equal(want) {
		t.Fatalf("created %v (unparsable manifest created must fall back to config)", rel.Created)
	}

	// created only RFC3339: a date-only value is ignored -> zero.
	r.put("app", "nocreated", mtManifest, image(map[string]string{"org.opencontainers.image.created": "2026-11-01"}, nil, nil))
	rel = resolve(t, s, "nocreated")
	if !rel.Created.IsZero() || rel.Version != nil || rel.Revision != nil || rel.MetadataUnresolved {
		t.Fatalf("%+v", rel)
	}
}

func TestResolveRequires(t *testing.T) {
	r := newReg(t, nil)
	p := RequiresAnnotationPrefix
	r.put("app", "req", mtManifest, image(
		map[string]string{p + "db": ">=2", p + "empty": "", p + "": "ignored", "other": "x"},
		map[string]string{p + "db": ">=1", p + "cache": "^1"}, nil))
	s := newSrc(t, r, "app", nil, nil)
	rel := resolve(t, s, "req")
	want := map[string]string{"db": ">=2", "empty": "", "cache": "^1"}
	if len(rel.Requires) != len(want) {
		t.Fatalf("%v", rel.Requires)
	}
	for k, v := range want {
		if got, ok := rel.Requires[k]; !ok || got != v {
			t.Fatalf("requires[%s]=%q ok=%v", k, got, ok)
		}
	}
}

func TestResolveBoundsFailClosed(t *testing.T) {
	r := newReg(t, nil)
	p := RequiresAnnotationPrefix
	s := newSrc(t, r, "app", nil, nil)

	many := map[string]string{}
	for i := 0; i < 33; i++ {
		many[fmt.Sprintf("%sc%d", p, i)] = "1"
	}
	exactly := map[string]string{}
	for i := 0; i < 32; i++ {
		exactly[fmt.Sprintf("%sc%d", p, i)] = "1"
	}
	r.put("app", "toomany", mtManifest, image(many, nil, nil))
	r.put("app", "exact32", mtManifest, image(exactly, nil, nil))
	r.put("app", "longkey", mtManifest, image(map[string]string{p + strings.Repeat("k", 65): "1"}, nil, nil))
	r.put("app", "okkey", mtManifest, image(map[string]string{p + strings.Repeat("k", 64): strings.Repeat("v", 256)}, nil, nil))
	r.put("app", "longval", mtManifest, image(nil, map[string]string{p + "k": strings.Repeat("v", 257)}, nil))

	for _, tag := range []string{"toomany", "longkey", "longval"} {
		rel, err := s.Resolve(context.Background(), source.Ref{Tag: tag})
		if err != nil {
			t.Fatalf("%s: bounds violation must not be an error: %v", tag, err)
		}
		if msg, ok := s.Rejection(tag); !ok || !strings.Contains(msg, tag) {
			t.Fatalf("%s: rejection not recorded: %q", tag, msg)
		}
		if !rel.MetadataUnresolved || rel.Digest == nil || rel.Requires != nil || rel.Tag != tag {
			t.Fatalf("%s: %+v", tag, rel)
		}
	}
	for _, tag := range []string{"exact32", "okkey"} {
		if rel := resolve(t, s, tag); rel.MetadataUnresolved {
			t.Fatalf("%s at the bound must resolve", tag)
		}
	}
}

func TestResolveUnreadableIsUnresolved(t *testing.T) {
	r := newReg(t, nil)
	s := newSrc(t, r, "app", nil, nil)
	rel := resolve(t, s, "missing")
	if !rel.MetadataUnresolved || rel.Digest != nil || rel.Tag != "missing" {
		t.Fatalf("%+v", rel)
	}
	// not JSON at all: digest known, metadata unresolved, nil error.
	req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/v2/app/manifests/junk", strings.NewReader("{not json"))
	req.Header.Set("Content-Type", mtManifest)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	rel = resolve(t, s, "junk")
	if resp.StatusCode/100 == 2 && (!rel.MetadataUnresolved || rel.Digest == nil) {
		t.Fatalf("%+v", rel)
	}
	// unreachable registry
	dead, _ := New(Config{Image: "127.0.0.1:1/app", Insecure: true})
	if rel := resolve(t, dead, "x"); !rel.MetadataUnresolved {
		t.Fatal("unreachable must be unresolved")
	}
}

func TestIndexDigestAndArtifact(t *testing.T) {
	r := newReg(t, nil)
	child := r.put("app", "child", mtManifest, image(nil, nil, nil))
	idx := m{"schemaVersion": 2, "mediaType": mtIndex,
		"manifests":   []any{m{"mediaType": mtManifest, "digest": child, "size": 1}},
		"annotations": map[string]string{"org.opencontainers.image.version": "idx-1", "org.opencontainers.image.title": "T"}}
	idxDigest := r.put("app", "multi", mtIndex, idx)
	s := newSrc(t, r, "app", nil, nil)

	rel := resolve(t, s, "multi")
	if rel.Digest == nil || *rel.Digest != idxDigest || *rel.Digest == child {
		t.Fatalf("index digest %v want %s", rel.Digest, idxDigest)
	}
	if rel.Version == nil || *rel.Version != "idx-1" {
		t.Fatalf("%+v", rel)
	}
	am, err := s.Artifact(context.Background(), source.Release{Tag: "multi"})
	if err != nil || am.Type != mtIndex || am.Title != "T" {
		t.Fatalf("%+v %v", am, err)
	}
	if rel := resolve(t, s, "child"); *rel.Digest != child {
		t.Fatal("single manifest digest")
	}
}

func TestArtifactMetadata(t *testing.T) {
	r := newReg(t, nil)
	s := newSrc(t, r, "app", nil, nil)
	ann := map[string]string{
		"org.opencontainers.image.source":      "https://github.com/o/r",
		"org.opencontainers.image.description": "d",
	}
	cfgAnn := map[string]string{"org.opencontainers.image.title": "cfgtitle", "org.opencontainers.image.source": "ignored"}
	// artifactType wins over config.mediaType
	r.put("app", "a1", mtManifest, image(ann, cfgAnn, m{"artifactType": "application/vnd.kuberik.x"}))
	// no artifactType: config.mediaType
	r.put("app", "a2", mtManifest, image(nil, nil, nil))
	// neither: mediaType (config.mediaType empty)
	r.put("app", "a3", mtManifest, m{"schemaVersion": 2, "mediaType": mtManifest,
		"config": m{"digest": "sha256:" + strings.Repeat("a", 64), "size": 2}, "layers": []any{}})

	// via Artifact without prior Resolve (fetch path), then via Resolve cache.
	for pass := 0; pass < 2; pass++ {
		am, err := s.Artifact(context.Background(), source.Release{Tag: "a1"})
		if err != nil || am.Type != "application/vnd.kuberik.x" || am.SourceURL != "https://github.com/o/r" || am.Title != "cfgtitle" || am.Description != "d" {
			t.Fatalf("a1 %+v %v", am, err)
		}
		resolve(t, s, "a1")
	}
	if am, _ := s.Artifact(context.Background(), source.Release{Tag: "a2"}); am.Type != "application/vnd.oci.image.config.v1+json" {
		t.Fatalf("a2 %+v", am)
	}
	if am, _ := s.Artifact(context.Background(), source.Release{Tag: "a3"}); am.Type != mtManifest {
		t.Fatalf("a3 %+v", am)
	}
}

func TestSecretRefAuth(t *testing.T) {
	r := newReg(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			u, p, ok := req.BasicAuth()
			if !ok || u != "u" || p != "p" {
				w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			h.ServeHTTP(w, req)
		})
	})
	r.put("app", "v1", mtManifest, image(map[string]string{"org.opencontainers.image.version": "1"}, nil, nil))

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	_ = AddToScheme(scheme)
	dcj := fmt.Sprintf(`{"auths":{%q:{"username":"u","password":"p"}}}`, r.host)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "creds"},
		Data: map[string][]byte{".dockerconfigjson": []byte(dcj)}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	rs := &v1alpha1.ReleaseSource{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "rs"},
		Spec: v1alpha1.ReleaseSourceSpec{OCI: &v1alpha1.OCISource{
			Image: r.host + "/app", Insecure: true, SecretRef: &corev1.LocalObjectReference{Name: "creds"}}}}
	src, err := Factory(context.Background(), c, rs)
	if err != nil {
		t.Fatal(err)
	}
	res, err := src.Discover(context.Background(), "")
	if err != nil || len(res.Refs) != 1 {
		t.Fatalf("authed discover: %v %v", res, err)
	}
	if rel, _ := src.Resolve(context.Background(), res.Refs[0]); rel.MetadataUnresolved {
		t.Fatal("authed resolve unresolved")
	}

	// no secretRef: anonymous -> 401 -> discover errors, resolve is unresolved
	rs2 := rs.DeepCopy()
	rs2.Spec.OCI.SecretRef = nil
	anon, _ := Factory(context.Background(), c, rs2)
	if _, err := anon.Discover(context.Background(), ""); err == nil {
		t.Fatal("anonymous must fail against basic-auth registry")
	}
	if rel, _ := anon.Resolve(context.Background(), source.Ref{Tag: "v1"}); !rel.MetadataUnresolved {
		t.Fatal("anonymous resolve must be unresolved")
	}

	// missing secret is a factory error
	rs3 := rs.DeepCopy()
	rs3.Spec.OCI.SecretRef.Name = "nope"
	if _, err := Factory(context.Background(), c, rs3); err == nil {
		t.Fatal("missing secret must error")
	}
}

func TestImagePolicyRef(t *testing.T) {
	r := newReg(t, nil)
	for _, tag := range []string{"main-1", "main-2", "other"} {
		r.put("app", tag, mtManifest, image(nil, nil, nil))
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	_ = AddToScheme(scheme)
	repo := &imagev1.ImageRepository{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "repo"},
		Spec: imagev1.ImageRepositorySpec{Image: r.host + "/app", Insecure: true, ExclusionList: []string{`^main-2$`}}}
	pol := &imagev1.ImagePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pol"},
		Spec: imagev1.ImagePolicySpec{
			ImageRepositoryRef: meta.NamespacedObjectReference{Name: "repo"},
			FilterTags:         &imagev1.TagFilter{Pattern: `^main-(?P<n>[0-9]+)$`, Extract: "$n"},
			Policy:             imagev1.ImagePolicyChoice{Numerical: &imagev1.NumericalPolicy{Order: "ASC"}},
		}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo, pol).Build()
	rs := &v1alpha1.ReleaseSource{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "rs"},
		Spec: v1alpha1.ReleaseSourceSpec{OCI: &v1alpha1.OCISource{ImagePolicyRef: &corev1.LocalObjectReference{Name: "pol"}}}}
	src, err := Factory(context.Background(), c, rs)
	if err != nil {
		t.Fatal(err)
	}
	res, err := src.Discover(context.Background(), "")
	if err != nil || len(res.Refs) != 1 || res.Refs[0].Tag != "main-1" { // main-2 is in exclusionList
		t.Fatalf("%v %v", res, err)
	}
	if !src.Less(source.Release{Tag: "main-1"}, source.Release{Tag: "main-2"}) {
		t.Fatal("order from ImagePolicy")
	}

	// missing policy / cross-namespace repo ref are errors
	rs.Spec.OCI.ImagePolicyRef.Name = "none"
	if _, err := Factory(context.Background(), c, rs); err == nil {
		t.Fatal("missing ImagePolicy must error")
	}
	pol2 := pol.DeepCopy()
	pol2.Name = "xns"
	pol2.ResourceVersion = ""
	pol2.Spec.ImageRepositoryRef.Namespace = "other"
	_ = c.Create(context.Background(), pol2)
	rs.Spec.OCI.ImagePolicyRef.Name = "xns"
	if _, err := Factory(context.Background(), c, rs); err == nil {
		t.Fatal("cross-namespace repo ref must error")
	}
}

func TestRegisteredAndNoSpec(t *testing.T) {
	if _, err := source.Lookup(Kind); err != nil {
		t.Fatal(err)
	}
	if _, err := Factory(context.Background(), fake.NewClientBuilder().Build(), &v1alpha1.ReleaseSource{}); err == nil {
		t.Fatal("no spec.oci must error")
	}
}

func TestResolveCRDBounds(t *testing.T) {
	r := newReg(t, nil)
	s := newSrc(t, r, "app", nil, nil)
	long := strings.Repeat("x", 129)
	r.put("app", "longver", mtManifest, image(map[string]string{"org.opencontainers.image.version": long}, nil, nil))
	r.put("app", "longrev", mtManifest, image(map[string]string{"org.opencontainers.image.revision": long}, nil, nil))
	r.put("app", "okver", mtManifest, image(map[string]string{"org.opencontainers.image.version": strings.Repeat("x", 128)}, nil, nil))
	for _, tag := range []string{"longver", "longrev"} {
		rel := resolve(t, s, tag)
		if !rel.MetadataUnresolved || rel.Version != nil || rel.Revision != nil || rel.Digest == nil {
			t.Fatalf("%s: %+v", tag, rel)
		}
		if msg, ok := s.Rejection(tag); !ok || !strings.Contains(msg, "exceeds") {
			t.Fatalf("%s: rejection %q", tag, msg)
		}
	}
	if rel := resolve(t, s, "okver"); rel.MetadataUnresolved || rel.Version == nil {
		t.Fatalf("version at the bound must resolve: %+v", rel)
	}
}

func TestDiscoverSkipsOverlongTags(t *testing.T) {
	r := newReg(t, nil)
	s := newSrc(t, r, "app", nil, nil)
	r.put("app", "v1", mtManifest, image(nil, nil, nil))
	r.put("app", strings.Repeat("t", 128), mtManifest, image(nil, nil, nil))
	r.put("app", strings.Repeat("t", 129), mtManifest, image(nil, nil, nil))
	res, err := s.Discover(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got := tagsOf(res); len(got) != 2 {
		t.Fatalf("tags: %v", got)
	}
	if n := s.SkippedLongTags(); n != 1 {
		t.Fatalf("skipped = %d", n)
	}
}

func TestClipArtifact(t *testing.T) {
	am := &v1alpha1.ArtifactMetadata{Type: "t", Title: strings.Repeat("x", 257), Description: strings.Repeat("d", 2048), SourceURL: strings.Repeat("u", 513)}
	clipArtifact(am)
	if am.Title != "" || am.SourceURL != "" || am.Type != "t" || len(am.Description) != 2048 {
		t.Fatalf("%+v", am)
	}
}
