package oci

import (
	"context"
	"errors"
	"fmt"

	imagev1 "github.com/fluxcd/image-reflector-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kuberik/release-controller/api/v1alpha1"
	"github.com/kuberik/release-controller/internal/source"
)

// Kind is the registration key of this source.
const Kind = "oci"

func init() { source.Register(Kind, Factory) }

// AddToScheme registers the Flux image.toolkit.fluxcd.io types the factory
// reads (ImagePolicy, ImageRepository). Call it on the manager's scheme.
func AddToScheme(s *runtime.Scheme) error { return imagev1.AddToScheme(s) }

// Factory builds the OCI source for a ReleaseSource. It reads only the spec of
// the referenced ImagePolicy and ImageRepository (never status) and the
// secretRef Secret, all in the ReleaseSource's namespace.
func Factory(ctx context.Context, c client.Reader, rs *v1alpha1.ReleaseSource) (source.Source, error) {
	cfg, err := ResolveConfig(ctx, c, rs)
	if err != nil {
		return nil, err
	}
	return New(cfg)
}

// ResolveConfig turns spec.oci into a Config, following imagePolicyRef.
func ResolveConfig(ctx context.Context, c client.Reader, rs *v1alpha1.ReleaseSource) (Config, error) {
	spec := rs.Spec.OCI
	if spec == nil {
		return Config{}, errors.New("oci: spec.oci is not set")
	}
	ns := rs.Namespace
	var cfg Config
	var secretName string

	if spec.ImagePolicyRef != nil {
		var pol imagev1.ImagePolicy
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: spec.ImagePolicyRef.Name}, &pol); err != nil {
			return Config{}, fmt.Errorf("failed to get ImagePolicy: %w", err)
		}
		ref := pol.Spec.ImageRepositoryRef
		if ref.Namespace != "" && ref.Namespace != ns {
			return Config{}, fmt.Errorf("ImagePolicy %s references ImageRepository in namespace %s; cross-namespace refs are not supported", pol.Name, ref.Namespace)
		}
		var repo imagev1.ImageRepository
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref.Name}, &repo); err != nil {
			return Config{}, fmt.Errorf("failed to get ImageRepository: %w", err)
		}
		cfg.Image = repo.Spec.Image
		cfg.Insecure = repo.Spec.Insecure
		if repo.Spec.SecretRef != nil {
			secretName = repo.Spec.SecretRef.Name
		}
		if pol.Spec.FilterTags != nil {
			cfg.FilterTags = &v1alpha1.TagFilter{Pattern: pol.Spec.FilterTags.Pattern, Extract: pol.Spec.FilterTags.Extract}
		}
		cfg.Policy = convertPolicy(pol.Spec.Policy)
	} else {
		cfg.Image = spec.Image
		cfg.Insecure = spec.Insecure
		cfg.FilterTags = spec.FilterTags
		cfg.Policy = spec.Policy
		if spec.SecretRef != nil {
			secretName = spec.SecretRef.Name
		}
	}

	var secret *corev1.Secret
	if secretName != "" {
		secret = &corev1.Secret{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: secretName}, secret); err != nil {
			return Config{}, fmt.Errorf("failed to get secret: %w", err)
		}
	}
	kc, err := keychainFromSecret(secret)
	if err != nil {
		return Config{}, err
	}
	cfg.Keychain = kc
	return cfg, nil
}

func convertPolicy(p imagev1.ImagePolicyChoice) *v1alpha1.OrderPolicy {
	switch {
	case p.SemVer != nil:
		return &v1alpha1.OrderPolicy{SemVer: &v1alpha1.SemVerPolicy{Range: p.SemVer.Range}}
	case p.Alphabetical != nil:
		return &v1alpha1.OrderPolicy{Alphabetical: &v1alpha1.AlphabeticalPolicy{Order: p.Alphabetical.Order}}
	case p.Numerical != nil:
		return &v1alpha1.OrderPolicy{Numerical: &v1alpha1.NumericalPolicy{Order: p.Numerical.Order}}
	}
	return nil
}
