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

package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sourcev1beta2 "github.com/fluxcd/source-controller/api/v1beta2"
	rolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

const (
	// RolloutNameAnnotation is the annotation key used to link an OCIRepository to a Rollout.
	RolloutNameAnnotation = "kuberik.com/rollout-name"
)

// FluxOCIRepositoryReconciler reconciles a Rollout object to update Flux OCIRepositories
type FluxOCIRepositoryReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=kuberik.com,resources=rollouts,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=ocirepositories,verbs=get;list;watch;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *FluxOCIRepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Get the Rollout that triggered the reconciliation
	rollout := rolloutv1alpha1.Rollout{}
	if err := r.Client.Get(ctx, req.NamespacedName, &rollout); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check if there's any deployment history
	if len(rollout.Status.History) == 0 {
		log.Info("No deployment history found, skipping OCIRepository update")
		return ctrl.Result{}, nil
	}

	// Get the latest deployment version
	latestDeployment := rollout.Status.History[0]

	// List all OCIRepositories in the same namespace as the Rollout
	var ociRepositories sourcev1beta2.OCIRepositoryList
	if err := r.Client.List(ctx, &ociRepositories, client.InNamespace(rollout.Namespace)); err != nil {
		log.Error(err, "Failed to list OCIRepositories in namespace", "namespace", rollout.Namespace)
		return ctrl.Result{}, err
	}

	for _, ociRepo := range ociRepositories.Items {
		// Check if the OCIRepository has the rollout name annotation and if it matches the current Rollout
		if annotationValue, ok := ociRepo.Annotations[RolloutNameAnnotation]; !ok || annotationValue != rollout.Name {
			continue
		}

		// OCIRepository is linked to this Rollout, proceed with update
		log.Info("Found matching OCIRepository", "name", ociRepo.Name, "rollout", rollout.Name)

		// Prepare the patch
		patch := client.MergeFrom(ociRepo.DeepCopy())

		// Ensure Spec.Reference is initialized
		if ociRepo.Spec.Reference == nil {
			ociRepo.Spec.Reference = &sourcev1beta2.OCIRepositoryRef{}
		}

		// Update the tag to the latest deployment version
		ociRepo.Spec.Reference.Tag = latestDeployment.Version
		// Ensure digest and semver are not set, so tag takes precedence
		ociRepo.Spec.Reference.Digest = ""
		ociRepo.Spec.Reference.SemVer = ""

		// Add reconcile annotation to trigger Flux reconciliation
		if ociRepo.Annotations == nil {
			ociRepo.Annotations = make(map[string]string)
		}
		ociRepo.Annotations["reconcile.fluxcd.io/requestedAt"] = metav1.Now().Format(time.RFC3339Nano)


		if err := r.Client.Patch(ctx, &ociRepo, patch, client.FieldOwner("kuberik-rollout-controller")); err != nil {
			log.Error(err, "Failed to patch OCIRepository", "name", ociRepo.Name)
			return ctrl.Result{}, err
		}

		log.Info("Successfully patched OCIRepository", "name", ociRepo.Name, "newTag", latestDeployment.Version)
		// Assuming only one OCIRepository should match, we can break early.
		// If multiple OCIRepositories could be linked, remove the break.
		break
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *FluxOCIRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rolloutv1alpha1.Rollout{}).
		Owns(&sourcev1beta2.OCIRepository{}). // Watch OCIRepositories for changes, useful for potential future logic
		Named("flux-ocirepository").
		Complete(r)
}
