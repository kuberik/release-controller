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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sourcev1beta2 "github.com/fluxcd/source-controller/api/v1beta2"
	rolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Flux OCIRepository Controller", func() {
	Context("When reconciling a Rollout", func() {
		const rolloutName = "test-rollout"
		const ociRepoName = "test-oci-repo"
		const newVersion = "1.0.1"

		ctx := context.Background()
		var namespace *corev1.Namespace
		var rolloutNamespacedName types.NamespacedName
		var ociRepoNamespacedName types.NamespacedName
		var testRollout *rolloutv1alpha1.Rollout
		var testOCIRepo *sourcev1beta2.OCIRepository

		var controllerReconciler *FluxOCIRepositoryReconciler

		BeforeEach(func() {
			By("creating a unique namespace for the test")
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-ns-",
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			namespace = ns

			rolloutNamespacedName = types.NamespacedName{
				Name:      rolloutName,
				Namespace: namespace.Name,
			}
			ociRepoNamespacedName = types.NamespacedName{
				Name:      ociRepoName,
				Namespace: namespace.Name,
			}

			controllerReconciler = &FluxOCIRepositoryReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
		})

		AfterEach(func() {
			By("Deleting the test namespace")
			Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
		})

		createRollout := func(withHistory bool) *rolloutv1alpha1.Rollout {
			r := &rolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rolloutName,
					Namespace: namespace.Name,
				},
				Spec: rolloutv1alpha1.RolloutSpec{
					ReleasesRepository: rolloutv1alpha1.Repository{ // This field is part of the spec but not used by this controller
						URL: "ghcr.io/test/releases",
					},
				},
			}
			Expect(k8sClient.Create(ctx, r)).To(Succeed())
			if withHistory {
				r.Status.History = []rolloutv1alpha1.DeploymentHistoryEntry{
					{Version: newVersion, Timestamp: metav1.Now()},
					{Version: "1.0.0", Timestamp: metav1.Time{Time: time.Now().Add(-time.Hour)}},
				}
				Expect(k8sClient.Status().Update(ctx, r)).To(Succeed())
			}
			return r
		}

		createOCIRepository := func(annotations map[string]string, initialRef *sourcev1beta2.OCIRepositoryRef) *sourcev1beta2.OCIRepository {
			oci := &sourcev1beta2.OCIRepository{
				ObjectMeta: metav1.ObjectMeta{
					Name:        ociRepoName,
					Namespace:   namespace.Name,
					Annotations: annotations,
				},
				Spec: sourcev1beta2.OCIRepositorySpec{
					URL:       "oci://ghcr.io/test/target",
					Interval:  metav1.Duration{Duration: time.Minute},
					Reference: initialRef,
				},
			}
			Expect(k8sClient.Create(ctx, oci)).To(Succeed())
			// Fetch to get the resource version
			createdOCI := &sourcev1beta2.OCIRepository{}
			Expect(k8sClient.Get(ctx, ociRepoNamespacedName, createdOCI)).To(Succeed())
			return createdOCI
		}

		reconcile := func() {
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: rolloutNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		}

		It("should update OCIRepository when Rollout has history and annotation matches", func() {
			By("Creating a Rollout with deployment history")
			testRollout = createRollout(true)

			By("Creating an OCIRepository with matching annotation")
			initialRef := &sourcev1beta2.OCIRepositoryRef{Tag: "old-tag", Digest: "sha256:olddigest", SemVer: "1.0.0"}
			testOCIRepo = createOCIRepository(map[string]string{RolloutNameAnnotation: rolloutName}, initialRef)

			By("Reconciling the Rollout")
			reconcile()

			By("Verifying the OCIRepository is updated")
			updatedOCIRepo := &sourcev1beta2.OCIRepository{}
			Expect(k8sClient.Get(ctx, ociRepoNamespacedName, updatedOCIRepo)).To(Succeed())

			Expect(updatedOCIRepo.Spec.Reference).NotTo(BeNil())
			Expect(updatedOCIRepo.Spec.Reference.Tag).To(Equal(newVersion))
			Expect(updatedOCIRepo.Spec.Reference.Digest).To(BeEmpty())
			Expect(updatedOCIRepo.Spec.Reference.SemVer).To(BeEmpty())
			Expect(updatedOCIRepo.Annotations).To(HaveKeyWithValue("reconcile.fluxcd.io/requestedAt", Not(BeEmpty())))
		})

		It("should not update OCIRepository if annotation value does not match Rollout name", func() {
			By("Creating a Rollout with deployment history")
			testRollout = createRollout(true)

			By("Creating an OCIRepository with a non-matching annotation value")
			initialRef := &sourcev1beta2.OCIRepositoryRef{Tag: "initial-tag"}
			testOCIRepo = createOCIRepository(map[string]string{RolloutNameAnnotation: "another-rollout"}, initialRef)
			originalVersion := testOCIRepo.ResourceVersion

			By("Reconciling the Rollout")
			reconcile()

			By("Verifying the OCIRepository is not modified")
			updatedOCIRepo := &sourcev1beta2.OCIRepository{}
			Expect(k8sClient.Get(ctx, ociRepoNamespacedName, updatedOCIRepo)).To(Succeed())
			Expect(updatedOCIRepo.ResourceVersion).To(Equal(originalVersion))
			Expect(updatedOCIRepo.Spec.Reference.Tag).To(Equal("initial-tag")) // Ensure tag didn't change
		})

		It("should not update OCIRepository if annotation is missing", func() {
			By("Creating a Rollout with deployment history")
			testRollout = createRollout(true)

			By("Creating an OCIRepository without the rollout annotation")
			initialRef := &sourcev1beta2.OCIRepositoryRef{Tag: "no-annotation-tag"}
			testOCIRepo = createOCIRepository(nil, initialRef) // No annotations
			originalVersion := testOCIRepo.ResourceVersion

			By("Reconciling the Rollout")
			reconcile()

			By("Verifying the OCIRepository is not modified")
			updatedOCIRepo := &sourcev1beta2.OCIRepository{}
			Expect(k8sClient.Get(ctx, ociRepoNamespacedName, updatedOCIRepo)).To(Succeed())
			Expect(updatedOCIRepo.ResourceVersion).To(Equal(originalVersion))
			Expect(updatedOCIRepo.Spec.Reference.Tag).To(Equal("no-annotation-tag"))
		})

		It("should not update OCIRepository if Rollout has no deployment history", func() {
			By("Creating a Rollout without deployment history")
			testRollout = createRollout(false)

			By("Creating an OCIRepository with matching annotation")
			initialRef := &sourcev1beta2.OCIRepositoryRef{Tag: "no-history-tag"}
			testOCIRepo = createOCIRepository(map[string]string{RolloutNameAnnotation: rolloutName}, initialRef)
			originalVersion := testOCIRepo.ResourceVersion

			By("Reconciling the Rollout")
			reconcile()

			By("Verifying the OCIRepository is not modified")
			updatedOCIRepo := &sourcev1beta2.OCIRepository{}
			Expect(k8sClient.Get(ctx, ociRepoNamespacedName, updatedOCIRepo)).To(Succeed())
			Expect(updatedOCIRepo.ResourceVersion).To(Equal(originalVersion))
			Expect(updatedOCIRepo.Spec.Reference.Tag).To(Equal("no-history-tag"))
		})

		It("should update OCIRepository correctly if spec.reference is initially nil", func() {
			By("Creating a Rollout with deployment history")
			testRollout = createRollout(true)

			By("Creating an OCIRepository with matching annotation and nil spec.reference")
			testOCIRepo = createOCIRepository(map[string]string{RolloutNameAnnotation: rolloutName}, nil) // Spec.Reference is nil

			By("Reconciling the Rollout")
			reconcile()

			By("Verifying the OCIRepository is updated and spec.reference initialized")
			updatedOCIRepo := &sourcev1beta2.OCIRepository{}
			Expect(k8sClient.Get(ctx, ociRepoNamespacedName, updatedOCIRepo)).To(Succeed())

			Expect(updatedOCIRepo.Spec.Reference).NotTo(BeNil())
			Expect(updatedOCIRepo.Spec.Reference.Tag).To(Equal(newVersion))
			Expect(updatedOCIRepo.Spec.Reference.Digest).To(BeEmpty())
			Expect(updatedOCIRepo.Spec.Reference.SemVer).To(BeEmpty())
			Expect(updatedOCIRepo.Annotations).To(HaveKeyWithValue("reconcile.fluxcd.io/requestedAt", Not(BeEmpty())))
		})
	})
})
