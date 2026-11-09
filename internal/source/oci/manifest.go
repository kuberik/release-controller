package oci

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RequiresAnnotationPrefix is the annotation key prefix of a declared
// requirement. Same value as rollout-controller's api RequiresAnnotationPrefix.
const RequiresAnnotationPrefix = "com.kuberik.rollout.requires."

// Bounds on the requires annotations one image may declare. They match the
// MaxProperties / CEL markers on Release.Requires, so a hostile or broken image
// cannot produce a slice the API server would reject. Exceeding any of them is
// an error rather than a truncation: dropping the excess would let Go's random
// map iteration push the real requirement out of the map, and a release with no
// recorded requirement is one a dependency admits.
const (
	maxRequiresAnnotations = 32
	maxRequiresKeyLength   = 64
	maxRequiresValueLength = 256
)

// MetadataRejectedError is returned (wrapped) by Resolve when an image's
// annotations violate the fail-closed bounds. The Release returned alongside it
// is still valid (tag, digest, MetadataUnresolved=true); the reconciler keeps
// it and emits a MetadataRejected Event from the error message.
type MetadataRejectedError struct {
	Tag    string
	Reason string
}

func (e *MetadataRejectedError) Error() string {
	return fmt.Sprintf("metadata rejected for tag %q: %s", e.Tag, e.Reason)
}

// metadata is what is read out of an OCI manifest. Moved from
// rollout-controller parseOCIManifest; behaviour is identical.
type metadata struct {
	Version      *string
	Revision     *string
	ArtifactType *string
	Source       *string
	Title        *string
	Description  *string
	Created      *metav1.Time
	Requires     map[string]string
}

type boundsError struct{ msg string }

func (e *boundsError) Error() string { return e.msg }

// parseManifest extracts metadata from raw manifest (or index) JSON. imageRef
// is only used in error messages.
func parseManifest(manifest []byte, imageRef string) (metadata, error) {
	var manifestData struct {
		MediaType    string            `json:"mediaType"`
		ArtifactType string            `json:"artifactType"`
		Annotations  map[string]string `json:"annotations"`
		Config       struct {
			MediaType   string            `json:"mediaType"`
			Annotations map[string]string `json:"annotations"`
		} `json:"config"`
	}
	if err := json.Unmarshal(manifest, &manifestData); err != nil {
		return metadata{}, fmt.Errorf("failed to parse manifest JSON for %s: %w", imageRef, err)
	}

	var versionStr, revisionStr, artifactTypeStr, sourceStr, titleStr, descriptionStr *string
	var createdTime *metav1.Time
	var requires map[string]string

	// Artifact type preference: artifactType, config.mediaType, manifest.mediaType.
	if manifestData.ArtifactType != "" {
		artifactTypeStr = &manifestData.ArtifactType
	} else if manifestData.Config.MediaType != "" {
		artifactTypeStr = &manifestData.Config.MediaType
	} else if manifestData.MediaType != "" {
		artifactTypeStr = &manifestData.MediaType
	}

	// requires: config annotations first, manifest annotations second (manifest
	// wins). An empty value is kept: dropping it would make a mis-templated
	// annotation indistinguishable from "declares no requirement".
	for _, annotations := range []map[string]string{manifestData.Config.Annotations, manifestData.Annotations} {
		for key, value := range annotations {
			contract, found := strings.CutPrefix(key, RequiresAnnotationPrefix)
			if !found || contract == "" {
				continue
			}
			if len(contract) > maxRequiresKeyLength || len(value) > maxRequiresValueLength {
				return metadata{}, &boundsError{fmt.Sprintf(
					"requires annotation for contract %q on %s exceeds the size limit (%d/%d bytes)",
					contract, imageRef, maxRequiresKeyLength, maxRequiresValueLength)}
			}
			if _, existing := requires[contract]; !existing && len(requires) >= maxRequiresAnnotations {
				return metadata{}, &boundsError{fmt.Sprintf(
					"%s declares more than %d requires annotations", imageRef, maxRequiresAnnotations)}
			}
			if requires == nil {
				requires = make(map[string]string)
			}
			requires[contract] = value
		}
	}

	// Manifest annotations first, config-descriptor annotations as per-field
	// fallback; empty values ignored; created parsed as RFC 3339 only.
	for _, annotations := range []map[string]string{manifestData.Annotations, manifestData.Config.Annotations} {
		if annotations == nil {
			continue
		}
		if versionStr == nil {
			if v, ok := annotations["org.opencontainers.image.version"]; ok && v != "" {
				versionStr = &v
			}
		}
		if revisionStr == nil {
			if r, ok := annotations["org.opencontainers.image.revision"]; ok && r != "" {
				revisionStr = &r
			}
		}
		if sourceStr == nil {
			if s, ok := annotations["org.opencontainers.image.source"]; ok && s != "" {
				sourceStr = &s
			}
		}
		if titleStr == nil {
			if t, ok := annotations["org.opencontainers.image.title"]; ok && t != "" {
				titleStr = &t
			}
		}
		if descriptionStr == nil {
			if d, ok := annotations["org.opencontainers.image.description"]; ok && d != "" {
				descriptionStr = &d
			}
		}
		if createdTime == nil {
			if c, ok := annotations["org.opencontainers.image.created"]; ok && c != "" {
				if parsed, err := time.Parse(time.RFC3339, c); err == nil {
					createdTime = &metav1.Time{Time: parsed}
				}
			}
		}
	}

	return metadata{
		Version:      versionStr,
		Revision:     revisionStr,
		ArtifactType: artifactTypeStr,
		Source:       sourceStr,
		Title:        titleStr,
		Description:  descriptionStr,
		Created:      createdTime,
		Requires:     requires,
	}, nil
}
