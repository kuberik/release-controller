// Package oci is the OCI release source: it lists the tags of a repository,
// applies Flux ImagePolicy filterTags/policy semantics, and reads release
// metadata out of image manifests exactly as rollout-controller does today.
package oci

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/source"
)

// Config is the resolved, source-independent configuration (from spec.oci, or
// from ImagePolicy+ImageRepository spec when imagePolicyRef is used).
type Config struct {
	Image      string
	Insecure   bool
	FilterTags *v1alpha1.TagFilter
	Policy     *v1alpha1.OrderPolicy
	// Exclude are regular expressions of tags to ignore (Flux
	// ImageRepository.spec.exclusionList; Flux scans drop matching tags before
	// the policy sees them).
	Exclude  []string
	Keychain authn.Keychain // nil means authn.DefaultKeychain
}

// Source implements source.Source for OCI repositories.
type Source struct {
	cfg Config
	ext *extractor
	exc []*regexp.Regexp
	pol *policy

	mu       sync.Mutex
	artifact map[string]*v1alpha1.ArtifactMetadata // tag -> metadata seen by Resolve
}

var _ source.Source = (*Source)(nil)

// New builds a Source from a resolved Config.
func New(cfg Config) (*Source, error) {
	if cfg.Image == "" {
		return nil, errors.New("oci: image is empty")
	}
	ext, err := newExtractor(cfg.FilterTags)
	if err != nil {
		return nil, err
	}
	pol, err := newPolicy(cfg.Policy)
	if err != nil {
		return nil, err
	}
	var exc []*regexp.Regexp
	for _, e := range cfg.Exclude {
		re, err := regexp.Compile(e)
		if err != nil {
			return nil, fmt.Errorf("invalid exclusion pattern %q: %w", e, err)
		}
		exc = append(exc, re)
	}
	if cfg.Keychain == nil {
		cfg.Keychain = authn.DefaultKeychain
	}
	return &Source{cfg: cfg, ext: ext, exc: exc, pol: pol, artifact: map[string]*v1alpha1.ArtifactMetadata{}}, nil
}

func (s *Source) opts() []crane.Option {
	o := []crane.Option{crane.WithAuthFromKeychain(s.cfg.Keychain)}
	if s.cfg.Insecure {
		o = append(o, crane.Insecure)
	}
	return o
}

// AdmitAll reports that no ordering policy is configured, so monotonic
// admission does not apply and every discovered tag is admitted. The reconciler
// detects this with an `interface{ AdmitAll() bool }` assertion.
func (s *Source) AdmitAll() bool { return s.pol.kind == policyNone }

// Discover lists all tags (crane.ListTags follows registry pagination), keeps
// those passing filterTags.pattern and, for semver/numerical, those orderable
// under the policy (semver: inside the range). There is no cursor.
func (s *Source) Discover(ctx context.Context, _ string) (source.DiscoverResult, error) {
	tags, err := crane.ListTags(s.cfg.Image, append(s.opts(), crane.WithContext(ctx))...)
	if err != nil {
		return source.DiscoverResult{}, fmt.Errorf("listing tags of %s: %w", s.cfg.Image, err)
	}
	refs := make([]source.Ref, 0, len(tags))
	for _, tag := range tags {
		if s.excluded(tag) {
			continue
		}
		extracted, ok := s.ext.apply(tag)
		if !ok || !s.pol.admits(extracted) {
			continue
		}
		refs = append(refs, source.Ref{Tag: tag})
	}
	return source.DiscoverResult{Refs: refs}, nil
}

// Resolve returns digest and manifest metadata for one tag.
//
// created contract: Release.Created is set only when the image carries an
// RFC 3339 org.opencontainers.image.created annotation. Otherwise it is left
// zero and the reconciler must set created=firstSeen, createdObserved=true.
//
// Failure contract: a registry or parse failure yields a Release with
// MetadataUnresolved=true and a nil error (the tag is kept, as today). A
// fail-closed bounds violation (>32 requires keys, key >64 B, value >256 B)
// yields the same Release (digest set if known) together with a non-nil
// *MetadataRejectedError; the reconciler must keep the Release, mark it
// unresolved, and emit a MetadataRejected Event with err.Error() (find it with
// errors.As). It must not fail the sync.
func (s *Source) Resolve(ctx context.Context, ref source.Ref) (source.Release, error) {
	rel := source.Release{Tag: ref.Tag, MetadataUnresolved: true}
	imageRef := s.cfg.Image + ":" + ref.Tag
	parsed, err := name.ParseReference(imageRef)
	if err != nil {
		return rel, nil
	}
	opts := append(s.opts(), crane.WithContext(ctx))

	desc, err := crane.Head(parsed.String(), opts...)
	if err != nil {
		return rel, nil
	}
	digest := desc.Digest.String()
	rel.Digest = &digest

	manifest, err := crane.Manifest(parsed.String(), opts...)
	if err != nil {
		return rel, nil
	}
	md, err := parseManifest(manifest, imageRef)
	if err != nil {
		var be *boundsError
		if errors.As(err, &be) {
			return rel, &MetadataRejectedError{Tag: ref.Tag, Reason: be.msg}
		}
		return rel, nil
	}

	rel.MetadataUnresolved = false
	rel.Version = md.Version
	rel.Revision = md.Revision
	rel.Requires = md.Requires
	if md.Created != nil {
		rel.Created = *md.Created
	} else {
		rel.Created = metav1.Time{}
	}

	am := &v1alpha1.ArtifactMetadata{}
	if md.ArtifactType != nil {
		am.Type = *md.ArtifactType
	}
	if md.Source != nil {
		am.SourceURL = *md.Source
	}
	if md.Title != nil {
		am.Title = *md.Title
	}
	if md.Description != nil {
		am.Description = *md.Description
	}
	s.mu.Lock()
	s.artifact[ref.Tag] = am
	s.mu.Unlock()
	return rel, nil
}

// Artifact returns the artifact-level metadata (artifactType, source, title,
// description) of a release, for status.artifact of the newest one. It reuses
// what Resolve already read in this Source instance and otherwise fetches the
// manifest. A nil result with a nil error means the manifest has none.
func (s *Source) Artifact(ctx context.Context, tag string) (*v1alpha1.ArtifactMetadata, error) {
	s.mu.Lock()
	am, ok := s.artifact[tag]
	s.mu.Unlock()
	if ok {
		return am, nil
	}
	imageRef := s.cfg.Image + ":" + tag
	parsed, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, err
	}
	manifest, err := crane.Manifest(parsed.String(), append(s.opts(), crane.WithContext(ctx))...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch manifest for %s: %w", imageRef, err)
	}
	md, err := parseManifest(manifest, imageRef)
	if err != nil {
		return nil, err
	}
	am = &v1alpha1.ArtifactMetadata{}
	if md.ArtifactType != nil {
		am.Type = *md.ArtifactType
	}
	if md.Source != nil {
		am.SourceURL = *md.Source
	}
	if md.Title != nil {
		am.Title = *md.Title
	}
	if md.Description != nil {
		am.Description = *md.Description
	}
	return am, nil
}

// Less orders releases under the policy, applied to the extracted tag value
// (the tag alone determines it, so no state is needed). With no policy, by
// created then tag.
func (s *Source) Less(a, b source.Release) bool {
	if s.pol.kind == policyNone {
		if !a.Created.Equal(&b.Created) {
			return a.Created.Before(&b.Created)
		}
		return a.Tag < b.Tag
	}
	ea, _ := s.ext.apply(a.Tag)
	eb, _ := s.ext.apply(b.Tag)
	return s.pol.less(ea, eb)
}

func (s *Source) excluded(tag string) bool {
	for _, re := range s.exc {
		if re.MatchString(tag) {
			return true
		}
	}
	return false
}
