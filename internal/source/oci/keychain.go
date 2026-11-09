package oci

import (
	"bytes"
	"fmt"

	"github.com/docker/cli/cli/config"
	"github.com/docker/cli/cli/config/configfile"
	"github.com/google/go-containerregistry/pkg/authn"
	corev1 "k8s.io/api/core/v1"
)

// dockerConfigKeychain implements authn.Keychain over a Docker config JSON.
// Moved verbatim from rollout-controller.
type dockerConfigKeychain struct {
	config *configfile.ConfigFile
}

func (k *dockerConfigKeychain) Resolve(resource authn.Resource) (authn.Authenticator, error) {
	for registry, auth := range k.config.AuthConfigs {
		if resource.RegistryStr() == registry {
			return authn.FromConfig(authn.AuthConfig{
				Username: auth.Username,
				Password: auth.Password,
			}), nil
		}
	}
	return authn.Anonymous, nil
}

// keychainFromSecret mirrors getImageRepositoryAuthentication: .dockerconfigjson
// from the secret if present, else authn.DefaultKeychain.
func keychainFromSecret(secret *corev1.Secret) (authn.Keychain, error) {
	if secret != nil {
		if raw, ok := secret.Data[".dockerconfigjson"]; ok {
			configFile, err := config.LoadFromReader(bytes.NewReader(raw))
			if err != nil {
				return nil, fmt.Errorf("failed to load Docker config: %w", err)
			}
			return &dockerConfigKeychain{config: configFile}, nil
		}
	}
	return authn.DefaultKeychain, nil
}
