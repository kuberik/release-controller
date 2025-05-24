/*
Copyright 2025.

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

// RolloutSpec defines the desired state of Rollout.
type RolloutSpec struct {
	// ReleasesRepository specifies the path to the releases repository
	// +kubebuilder:validation:Required
	// +required
	ReleasesRepository Repository `json:"releasesRepository,omitempty"`

	// WantedVersion specifies a specific version to deploy, overriding the automatic version selection
	// +optional
	WantedVersion *string `json:"wantedVersion,omitempty"`

	// VersionHistoryLimit defines the maximum number of entries to keep in the deployment history
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	// +optional
	VersionHistoryLimit *int32 `json:"versionHistoryLimit,omitempty"`

	// ReleaseUpdateInterval defines how often the available releases should be updated
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern="^([0-9]+(\\.[0-9]+)?(ms|s|m|h))+$"
	// +kubebuilder:default="1m"
	// +optional
	ReleaseUpdateInterval *metav1.Duration `json:"releaseUpdateInterval,omitempty"`

	// BakeTime specifies how long to wait after deployment before marking as successful
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern="^([0-9]+(\\.[0-9]+)?(ms|s|m|h))+$"
	// +optional
	BakeTime *metav1.Duration `json:"bakeTime,omitempty"`

	// HealthCheckSelector specifies the label selector for matching HealthChecks
	// +optional
	HealthCheckSelector *metav1.LabelSelector `json:"healthCheckSelector,omitempty"`
}

type Repository struct {
	// The URL of the repository
	// +kubebuilder:validation:Required
	// +required
	URL string `json:"url,omitempty"`

	// The secret name containing the authentication credentials
	// +optional
	Auth *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// RolloutGateStatusSummary summarizes the status of a gate relevant to this rollout.
type RolloutGateStatusSummary struct {
	// Name is the name of the gate.
	// +kubebuilder:validation:Required
	// +required
	Name string `json:"name"`

	// Passing is true if the gate is passing, false if it is blocking.
	// +optional
	Passing *bool `json:"passing,omitempty"`

	// AllowedVersions is a list of versions that are allowed by the gate.
	// +optional
	AllowedVersions []string `json:"allowedVersions,omitempty"`

	// Message is a message describing the status of the gate.
	// +optional
	Message string `json:"message,omitempty"`
}

// RolloutStatus defines the observed state of Rollout.
type RolloutStatus struct {
	// Conditions represents the current state of the rollout process.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// History tracks the deployment history of this Rollout.
	// Each entry contains the version deployed and the timestamp of the deployment.
	// +optional
	History []DeploymentHistoryEntry `json:"history,omitempty"`

	// AvailableReleases is a list of all releases available in the releases repository.
	// +optional
	// +listType=set
	AvailableReleases []string `json:"availableReleases,omitempty"`

	// Gates summarizes the status of each gate relevant to this rollout.
	// +optional
	Gates []RolloutGateStatusSummary `json:"gates,omitempty"`

	// BakeStartTime is the time when the current bake period started
	// +optional
	BakeStartTime *metav1.Time `json:"bakeStartTime,omitempty"`

	// BakeEndTime is the time when the current bake period ends
	// +optional
	BakeEndTime *metav1.Time `json:"bakeEndTime,omitempty"`
}

// DeploymentHistoryEntry represents a single entry in the deployment history.
type DeploymentHistoryEntry struct {
	// Version is the version that was deployed.
	// +kubebuilder:validation:Required
	// +required
	Version string `json:"version"`

	// Timestamp is the time when the deployment occurred.
	// +kubebuilder:validation:Required
	// +required
	Timestamp metav1.Time `json:"timestamp"`

	// BakeStatus tracks the bake state for this deployment (e.g., None, InProgress, Succeeded, Failed)
	// +optional
	BakeStatus *string `json:"bakeStatus,omitempty"`

	// BakeStatusMessage provides details about the bake state for this deployment
	// +optional
	BakeStatusMessage *string `json:"bakeStatusMessage,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.history[0].version`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.releasesRepository.url`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description=""
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status",description=""
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].message",description=""

// Rollout is the Schema for the rollouts API.
type Rollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RolloutSpec   `json:"spec,omitempty"`
	Status RolloutStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RolloutList contains a list of Rollout.
type RolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Rollout `json:"items"`
}

const (
	// RolloutReady means the rollout is ready to serve requests.
	RolloutReady = "Ready"
	// RolloutReleasesUpdated means the available releases were updated.
	RolloutReleasesUpdated = "ReleasesUpdated"
	// RolloutGatesPassing means all gates are passing.
	RolloutGatesPassing = "GatesPassing"
)

const (
	BakeStatusInProgress = "InProgress"
	BakeStatusSucceeded  = "Succeeded"
	BakeStatusFailed     = "Failed"
)

func init() {
	SchemeBuilder.Register(&Rollout{}, &RolloutList{})
}
