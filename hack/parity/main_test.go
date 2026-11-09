package main

import (
	imagev1 "github.com/fluxcd/image-reflector-controller/api/v1"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/source"
	"github.com/kuberik/release-controller/internal/source/oci"
)

// A retagged deployed tag that left the catalogue is still judged by the
// reader fallback rule (3a.9): WARN only when the fallback has candidates,
// and candidates whose created is only the observed time do not count.
func TestDeployedRetaggedTagIsJudgedByTheFallbackRule(t *testing.T) {
	src, err := oci.New(oci.Config{Image: "x/y", Policy: &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{}}})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	then, now := t0, t0.Add(time.Hour) // deployed tag was created at t0, pushed again an hour later
	mk := func(cat ...source.Release) (*rollout, *built) {
		r := &rollout{}
		r.Metadata.CreationTimestamp = metav1.NewTime(now.Add(time.Hour)) // extras then read as predates-rollout
		r.Status.Available = []todayRelease{{Tag: "a", Created: &then}}
		r.Status.History = []struct {
			Version struct {
				Tag     string       `json:"tag"`
				Created *metav1.Time `json:"created,omitempty"`
			} `json:"version"`
		}{{}}
		r.Status.History[0].Version.Tag = "a"
		hc := metav1.NewTime(then)
		r.Status.History[0].Version.Created = &hc
		c := &built{
			src:    src,
			listed: map[string]bool{"a": true},
			rels:   map[string]source.Release{"a": {Tag: "a", Created: metav1.NewTime(now)}},
			ever:   map[string]source.Release{},
			final:  cat,
		}
		return r, c
	}
	b := &baseline{policies: map[string]*imagev1.ImagePolicy{}}

	// no candidate created after the deployed entry: FAIL, even though retagged
	r, c := mk(source.Release{Tag: "b", Created: metav1.NewTime(then.Add(-time.Hour))})
	if got := check("ns/r", r, c, b, now); got != "FAIL" {
		t.Fatalf("no fallback candidates: got %s, want FAIL", got)
	}
	// the only newer release has created=firstSeen: not a candidate
	r, c = mk(source.Release{Tag: "b", Created: metav1.NewTime(now), CreatedObserved: true})
	if got := check("ns/r", r, c, b, now); got != "FAIL" {
		t.Fatalf("observed-only candidate: got %s, want FAIL", got)
	}
	// a real newer release: WARN
	r, c = mk(source.Release{Tag: "b", Created: metav1.NewTime(now)})
	if got := check("ns/r", r, c, b, now); got != "WARN" {
		t.Fatalf("real candidate: got %s, want WARN", got)
	}
}
