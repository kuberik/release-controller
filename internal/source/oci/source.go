// Package oci is the OCI release source: it lists the tags of a repository,
// applies Flux ImagePolicy filterTags/policy semantics, and reads release
// metadata out of image manifests exactly as rollout-controller does today.
package oci

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

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

	mu          sync.Mutex
	artifact    map[string]*v1alpha1.ArtifactMetadata // tag -> metadata seen by Resolve
	rejected    map[string]string                     // tag -> bounds-violation message
	skippedLong int                                   // tags skipped by the last Discover for exceeding maxTagLength
}

var (
	_ source.Source           = (*Source)(nil)
	_ source.AdmitAll         = (*Source)(nil)
	_ source.ArtifactProvider = (*Source)(nil)
	_ source.Rejecter         = (*Source)(nil)
	_ source.TagSkipper       = (*Source)(nil)
)

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
	return &Source{cfg: cfg, ext: ext, exc: exc, pol: pol, artifact: map[string]*v1alpha1.ArtifactMetadata{}, rejected: map[string]string{}}, nil
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
	tooLong := 0
	for _, tag := range tags {
		if len(tag) > maxTagLength {
			tooLong++
			continue
		}
		if s.excluded(tag) {
			continue
		}
		extracted, ok := s.ext.apply(tag)
		if !ok || !s.pol.admits(extracted) {
			continue
		}
		refs = append(refs, source.Ref{Tag: tag})
	}
	if tooLong > 0 {
		s.mu.Lock()
		s.skippedLong = tooLong
		s.mu.Unlock()
		ctrl.LoggerFrom(ctx).Info("skipped tags longer than the CRD bound", "image", s.cfg.Image, "count", tooLong, "max", maxTagLength)
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
// yields the same (digest set if known) with a nil error as well, so the tag
// is not retried every sync; the reason is recorded and available from
// Rejection(tag) so the reconciler can emit a MetadataRejected Event.
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
			s.mu.Lock()
			s.rejected[ref.Tag] = (&MetadataRejectedError{Tag: ref.Tag, Reason: be.msg}).Error()
			s.mu.Unlock()
			return rel, nil
		}
		return rel, nil
	}

	rel.MetadataUnresolved = false
	rel.Version = md.Version
	rel.Revision = md.Revision
	rel.Requires = md.Requires
	// Bounds of the ReleaseSlice CRD: an over-long field would fail the whole
	// slice write, so treat it as a violation like requires (fail closed).
	var violations []string
	if len(digest) > maxDigestLength {
		rel.Digest = nil
		violations = append(violations, fmt.Sprintf("digest exceeds %d bytes", maxDigestLength))
	}
	if rel.Version != nil && len(*rel.Version) > maxVersionLength {
		rel.Version = nil
		violations = append(violations, fmt.Sprintf("version exceeds %d bytes", maxVersionLength))
	}
	if rel.Revision != nil && len(*rel.Revision) > maxRevisionLength {
		rel.Revision = nil
		violations = append(violations, fmt.Sprintf("revision exceeds %d bytes", maxRevisionLength))
	}
	if len(violations) > 0 {
		rel.MetadataUnresolved = true
		rel.Requires = nil
		rel.Created = metav1.Time{}
		s.mu.Lock()
		s.rejected[ref.Tag] = (&MetadataRejectedError{Tag: ref.Tag, Reason: strings.Join(violations, "; ")}).Error()
		s.mu.Unlock()
		return rel, nil
	}
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
	clipArtifact(am)
	s.mu.Lock()
	s.artifact[ref.Tag] = am
	s.mu.Unlock()
	return rel, nil
}

// SkippedLongTags is the number of tags the last Discover skipped because they
// exceed the CRD tag bound.
func (s *Source) SkippedLongTags() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.skippedLong
}

// Rejection returns the MetadataRejected message recorded by Resolve for tag
// in this Source instance (bounds violation), if any.
func (s *Source) Rejection(tag string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.rejected[tag]
	return msg, ok
}

// Artifact implements source.ArtifactProvider: artifact-level metadata
// (artifactType, source, title, description) of the newest release, for
// status.artifact. It reuses what Resolve already read in this Source instance
// and otherwise fetches the manifest. A nil result with a nil error means the
// manifest has none.
func (s *Source) Artifact(ctx context.Context, newest source.Release) (*v1alpha1.ArtifactMetadata, error) {
	tag := newest.Tag
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
	clipArtifact(am)
	return am, nil
}

// CRD bounds on the status/slice fields the source fills.
const (
	maxTagLength      = 128
	maxVersionLength  = 128
	maxRevisionLength = 128
	maxDigestLength   = 140
)

// clipArtifact drops artifact fields over their status.artifact CRD bounds
// (they are descriptive only; an over-long one must not fail the status write).
func clipArtifact(am *v1alpha1.ArtifactMetadata) {
	if len(am.Type) > 256 {
		am.Type = ""
	}
	if len(am.SourceURL) > 512 {
		am.SourceURL = ""
	}
	if len(am.Title) > 256 {
		am.Title = ""
	}
	if len(am.Description) > 2048 {
		am.Description = ""
	}
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
