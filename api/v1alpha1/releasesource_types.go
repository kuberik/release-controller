/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ReleaseSourceLabel is set on every ReleaseSlice and holds the name of
	// the owning ReleaseSource.
	ReleaseSourceLabel = "kuberik.com/release-source"

	// ReadyCondition: the catalogue is usable by readers.
	ReadyCondition = "Ready"
	// SourceReachableCondition: the upstream (registry) answered the last sync.
	SourceReachableCondition = "SourceReachable"
	// SyncingCondition: the resolve budget was not yet drained.
	SyncingCondition = "Syncing"
)

// ReleaseSourceSpec is a discriminated union: exactly one source member must
// be set. v1 has only oci.
type ReleaseSourceSpec struct {
	// OCI lists tags of an OCI repository.
	// +optional
	OCI *OCISource `json:"oci,omitempty"`

	// Interval between syncs. Jittered +-10% by the controller.
	// +kubebuilder:default="1m"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('10s')",message="interval must be at least 10s"
	// +optional
	Interval metav1.Duration `json:"interval,omitempty"`

	// Suspend stops syncing while true.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// Retention controls when whole old slices are dropped.
	// +kubebuilder:default={}
	// +optional
	Retention Retention `json:"retention,omitempty"`
}

// Retention mirrors Rollout.availableReleasesRetentionDays / MinCount.
type Retention struct {
	// Days after which a release counts as expired. 0 means age never expires anything.
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=0
	// +optional
	Days *int32 `json:"days,omitempty"`
	// MinCount is the number of newest releases always kept.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinCount *int32 `json:"minCount,omitempty"`
}

// OCISource configures an OCI repository source.
// +kubebuilder:validation:XValidation:rule="has(self.image) != has(self.imagePolicyRef)",message="exactly one of image or imagePolicyRef must be set"
type OCISource struct {
	// Image is the repository, e.g. ghcr.io/org/app.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Image string `json:"image,omitempty"`
	// SecretRef names a kubernetes.io/dockerconfigjson Secret.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	// Insecure allows plain HTTP registries.
	// +optional
	Insecure bool `json:"insecure,omitempty"`
	// FilterTags has the Flux ImagePolicy shape.
	// +optional
	FilterTags *TagFilter `json:"filterTags,omitempty"`
	// Policy has the Flux ImagePolicy shape.
	// +optional
	Policy *OrderPolicy `json:"policy,omitempty"`
	// ImagePolicyRef reads image, secretRef, filterTags and policy from an
	// existing Flux ImagePolicy and its ImageRepository (spec only).
	// +optional
	ImagePolicyRef *corev1.LocalObjectReference `json:"imagePolicyRef,omitempty"`
}

// TagFilter filters tags by regular expression (Flux ImagePolicy shape).
type TagFilter struct {
	// Pattern is a regular expression tags must match.
	// +optional
	Pattern string `json:"pattern,omitempty"`
	// Extract is a capture-group expression applied before ordering.
	// +optional
	Extract string `json:"extract,omitempty"`
}

// OrderPolicy picks how tags are ordered (Flux ImagePolicy shape). At most one member.
// +kubebuilder:validation:MaxProperties=1
type OrderPolicy struct {
	// +optional
	SemVer *SemVerPolicy `json:"semver,omitempty"`
	// +optional
	Alphabetical *AlphabeticalPolicy `json:"alphabetical,omitempty"`
	// +optional
	Numerical *NumericalPolicy `json:"numerical,omitempty"`
}

// SemVerPolicy orders tags by semantic version within a range.
type SemVerPolicy struct {
	// +kubebuilder:validation:MinLength=1
	Range string `json:"range"`
}

// AlphabeticalPolicy orders tags alphabetically.
type AlphabeticalPolicy struct {
	// +kubebuilder:default=asc
	// +kubebuilder:validation:Enum=asc;desc
	// +optional
	Order string `json:"order,omitempty"`
}

// NumericalPolicy orders tags numerically.
type NumericalPolicy struct {
	// +kubebuilder:default=asc
	// +kubebuilder:validation:Enum=asc;desc
	// +optional
	Order string `json:"order,omitempty"`
}

// ReleaseRef identifies the newest release.
type ReleaseRef struct {
	Tag string `json:"tag"`
	// +optional
	Created *metav1.Time `json:"created,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	// +optional
	Revision string `json:"revision,omitempty"`
}

// ArtifactMetadata is the artifact-level metadata of the newest release.
type ArtifactMetadata struct {
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Type string `json:"type,omitempty"`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	SourceURL string `json:"sourceURL,omitempty"`
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Title string `json:"title,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	Description string `json:"description,omitempty"`
}

// ReleaseSourceStatus defines the observed state of ReleaseSource.
type ReleaseSourceStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Newest is the newest release in the catalogue.
	// +optional
	Newest *ReleaseRef `json:"newest,omitempty"`
	// Count of releases in the catalogue.
	// +optional
	Count int32 `json:"count,omitempty"`
	// Slices oldest to newest; the last is open. Convenience only: readers select by label.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	Slices []string `json:"slices,omitempty"`
	// +optional
	NextSliceIndex int32 `json:"nextSliceIndex,omitempty"`
	// Artifact metadata of the newest release.
	// +optional
	Artifact *ArtifactMetadata `json:"artifact,omitempty"`
	// Cursor is source-opaque (e.g. an ETag).
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Cursor string `json:"cursor,omitempty"`
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// Conditions: Ready, SourceReachable, Syncing.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=relsrc
// +kubebuilder:printcolumn:name="Newest",type=string,JSONPath=`.status.newest.tag`
// +kubebuilder:printcolumn:name="Count",type=integer,JSONPath=`.status.count`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 230",message="name must be at most 230 characters so slice names fit 253"
// +kubebuilder:validation:XValidation:rule="has(self.spec.oci)",message="exactly one source must be set"

// ReleaseSource is the Schema for the releasesources API.
type ReleaseSource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec ReleaseSourceSpec `json:"spec"`
	// +optional
	Status ReleaseSourceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ReleaseSourceList contains a list of ReleaseSource.
type ReleaseSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ReleaseSource `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ReleaseSource{}, &ReleaseSourceList{})
}
