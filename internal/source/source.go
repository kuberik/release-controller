// Package source defines the pluggable release-discovery interface. The
// reconciler owns diffing, admission, resolve budget, slice writes and
// retention; a source only lists and resolves tags.
package source

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kuberik/release-controller/api/v1alpha1"
)

// Release is the catalogue entry; created/firstSeen are filled by the
// framework where the source has none.
type Release = v1alpha1.Release

// DiscoverResult is what a listing returns.
type DiscoverResult struct {
	// Refs is every candidate the source currently has.
	Refs []Ref
	// Cursor is source-opaque (e.g. an ETag), passed back next time.
	Cursor string
	// NotModified means nothing changed since the cursor.
	NotModified bool
}

// Ref is a tag plus whatever the listing carried for free.
type Ref struct {
	Tag string
	// Partial is non-nil when the listing already carries metadata; nil for OCI.
	Partial *Release
}

// Source discovers and resolves releases.
type Source interface {
	// Discover lists the source. Must be cheap and safe to call every interval.
	Discover(ctx context.Context, cursor string) (DiscoverResult, error)
	// Resolve returns full metadata for one tag. A transient failure returns a
	// Release with MetadataUnresolved=true and a nil error; only a permanent
	// policy failure (e.g. requires annotations over the bound) returns an error.
	Resolve(ctx context.Context, ref Ref) (Release, error)
	// Less orders two releases under the source's policy. It is the only thing
	// admission looks at for tags that are not resolved yet, so it must work on
	// tag stubs (Release{Tag}) or the listing's Partial. A policy is required.
	Less(a, b Release) bool
}

// Identifier is an optional interface. Identity returns what the catalogue is
// built from after references are followed (OCI: the repository of the
// ImagePolicy's ImageRepository). The reconciler stores it in the identity
// annotation so a referenced object changing its image rebuilds the catalogue.
type Identifier interface {
	Identity() string
}

// Rejecter is an optional interface. After Resolve, a source that recorded a
// fail-closed rejection (e.g. metadata over the CRD bounds) returns its message
// and true; the reconciler emits a Warning Event MetadataRejected.
type Rejecter interface {
	Rejection(tag string) (string, bool)
}

// TagSkipper is an optional interface. After Discover, SkippedLongTags is the
// number of tags the source dropped because they cannot be stored (longer than
// the ReleaseSlice tag bound); the reconciler emits a Warning Event TagsSkipped.
type TagSkipper interface {
	SkippedLongTags() int
}

// ArtifactProvider is an optional interface. The reconciler calls it when the
// newest release changes and copies the result to status.artifact. An error
// or nil result keeps the previous status.artifact.
type ArtifactProvider interface {
	Artifact(ctx context.Context, newest Release) (*v1alpha1.ArtifactMetadata, error)
}

// Factory builds a Source from a ReleaseSource; it reads secrets and
// referenced objects through the given namespace-scoped client.
type Factory func(ctx context.Context, c client.Reader, rs *v1alpha1.ReleaseSource) (Source, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a factory for a source kind ("oci" in v1). It panics on a
// duplicate or nil factory, like database/sql.Register.
func Register(kind string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if f == nil {
		panic("source: Register factory is nil for " + kind)
	}
	if _, dup := factories[kind]; dup {
		panic("source: Register called twice for " + kind)
	}
	factories[kind] = f
}

// Lookup returns the factory registered for kind.
func Lookup(kind string) (Factory, error) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := factories[kind]
	if !ok {
		return nil, fmt.Errorf("source: no factory registered for kind %q", kind)
	}
	return f, nil
}

// Kinds returns the registered kinds, sorted.
func Kinds() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
