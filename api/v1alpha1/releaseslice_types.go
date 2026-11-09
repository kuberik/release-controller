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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Release is one catalogue entry. It is a superset of rollout-controller's
// VersionInfo (tag, digest, version, revision, created, requires,
// metadataUnresolved).
//
// CEL cost: Requires and Metadata sit inside a list with maxItems 1000, so
// their rules are written as a single self.all(k, ...) over a bounded map
// (maxProperties 32 / 8) and string maxLengths are declared on the values'
// schema so the apiserver can estimate cost.
type Release struct {
	// Tag is the deployable reference. Unique per source.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Tag string `json:"tag"`
	// +kubebuilder:validation:MaxLength=140
	// +optional
	Digest *string `json:"digest,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	// +optional
	Version *string `json:"version,omitempty"`
	// Revision is a git SHA by contract.
	// +kubebuilder:validation:MaxLength=128
	// +optional
	Revision *string `json:"revision,omitempty"`
	// Created is source-provided, else firstSeen (createdObserved=true).
	Created metav1.Time `json:"created"`
	// +optional
	CreatedObserved bool `json:"createdObserved,omitempty"`
	// FirstSeen is when this catalogue first admitted the release.
	FirstSeen metav1.Time `json:"firstSeen"`
	// Requires: keys <= 64, values <= 256 (same as VersionInfo.Requires).
	// +kubebuilder:validation:MaxProperties=32
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) <= 64 && size(self[k]) <= 256)",message="requires keys must be at most 64 and values at most 256 characters"
	// +optional
	Requires map[string]string `json:"requires,omitempty"`
	// +optional
	MetadataUnresolved bool `json:"metadataUnresolved,omitempty"`
	// URL of the release page.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	URL *string `json:"url,omitempty"`
	// Metadata is source-specific and small; OCI v1 leaves it empty.
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) <= 63 && size(self[k]) <= 256)",message="metadata keys must be at most 63 and values at most 256 characters"
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=relslice
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.source`
// +kubebuilder:printcolumn:name="Sealed",type=boolean,JSONPath=`.sealed`
// +kubebuilder:printcolumn:name="Newest",type=date,JSONPath=`.newestCreated`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ReleaseSlice holds a bounded chunk of a ReleaseSource catalogue. Like
// EndpointSlice it has no spec/status. Labelled kuberik.com/release-source,
// owned (controller=true) by its ReleaseSource, named <source>-<5-digit index>.
type ReleaseSlice struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Source is the owning ReleaseSource name. Immutable.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="source is immutable"
	Source string `json:"source"`
	// Sealed slices take no new releases. Once true stays true.
	// +kubebuilder:validation:XValidation:rule="!oldSelf || self",message="sealed cannot go from true to false"
	// Always serialised (no omitempty) and defaulted to false so the
	// transition rule also sees true -> false from clients that drop zero values.
	// +kubebuilder:default=false
	Sealed bool `json:"sealed"`
	// +optional
	OldestCreated *metav1.Time `json:"oldestCreated,omitempty"`
	// +optional
	NewestCreated *metav1.Time `json:"newestCreated,omitempty"`
	// Releases sorted by (created, tag).
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=1000
	Releases []Release `json:"releases"`
}

// +kubebuilder:object:root=true

// ReleaseSliceList contains a list of ReleaseSlice.
type ReleaseSliceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ReleaseSlice `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ReleaseSlice{}, &ReleaseSliceList{})
}
