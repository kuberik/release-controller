package controller

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
)

// CRD validation: these rules are the contract rollout-controller mirrors.
var _ = Describe("CRD validation", func() {
	ctx := context.Background()
	now := metav1.Now()

	rel := func(i int) v1alpha1.Release {
		return v1alpha1.Release{Tag: fmt.Sprintf("t-%d", i), Created: now, FirstSeen: now}
	}
	slice := func(name string, n int) *v1alpha1.ReleaseSlice {
		s := &v1alpha1.ReleaseSlice{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Source:     "src",
		}
		for i := 0; i < n; i++ {
			s.Releases = append(s.Releases, rel(i))
		}
		return s
	}

	Context("ReleaseSlice", func() {
		It("accepts 1000 minimal releases and rejects 1001", func() {
			Expect(k8sClient.Create(ctx, slice("v-1000", 1000))).To(Succeed())
			Expect(k8sClient.Create(ctx, slice("v-1001", 1001))).NotTo(Succeed())
		})

		It("rejects sealed true->false and any source change", func() {
			s := slice("v-seal", 1)
			s.Sealed = true
			Expect(k8sClient.Create(ctx, s)).To(Succeed())

			s.Sealed = false // Go client sends sealed:false (no omitempty)
			Expect(k8sClient.Update(ctx, s)).NotTo(Succeed())

			Expect(k8sClient.Get(ctx, clientKey(s), s)).To(Succeed())
			s.Source = "other"
			Expect(k8sClient.Update(ctx, s)).NotTo(Succeed())
		})

		It("accepts sealed false->true", func() {
			s := slice("v-open", 1)
			Expect(k8sClient.Create(ctx, s)).To(Succeed())
			s.Sealed = true
			Expect(k8sClient.Update(ctx, s)).To(Succeed())
		})

		It("rejects a 129-char tag and accepts 128", func() {
			s := slice("v-tag", 1)
			s.Releases[0].Tag = strings.Repeat("a", 129)
			Expect(k8sClient.Create(ctx, s)).NotTo(Succeed())
			s.Releases[0].Tag = strings.Repeat("a", 128)
			Expect(k8sClient.Create(ctx, s)).To(Succeed())
		})

		It("bounds requires: 33 keys, long key, long value", func() {
			mk := func(name string, m map[string]string) error {
				s := slice(name, 1)
				s.Releases[0].Requires = m
				return k8sClient.Create(ctx, s)
			}
			many := map[string]string{}
			for i := 0; i < 33; i++ {
				many[fmt.Sprintf("k%d", i)] = "v"
			}
			Expect(mk("v-req33", many)).NotTo(Succeed())
			delete(many, "k0")
			Expect(mk("v-req32", many)).To(Succeed())
			Expect(mk("v-reqkey", map[string]string{strings.Repeat("k", 65): "v"})).NotTo(Succeed())
			Expect(mk("v-reqval", map[string]string{"k": strings.Repeat("v", 257)})).NotTo(Succeed())
			Expect(mk("v-reqok", map[string]string{strings.Repeat("k", 64): strings.Repeat("v", 256)})).To(Succeed())
		})
	})

	Context("ReleaseSource", func() {
		mk := func(name string, oci *v1alpha1.OCISource) error {
			return k8sClient.Create(ctx, &v1alpha1.ReleaseSource{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec:       v1alpha1.ReleaseSourceSpec{OCI: oci},
			})
		}
		ref := &corev1.LocalObjectReference{Name: "p"}

		It("requires exactly one of image / imagePolicyRef", func() {
			Expect(mk("s-neither", &v1alpha1.OCISource{})).NotTo(Succeed())
			Expect(mk("s-both", &v1alpha1.OCISource{Image: "a/b", ImagePolicyRef: ref})).NotTo(Succeed())
			Expect(mk("s-image", &v1alpha1.OCISource{Image: "a/b"})).To(Succeed())
			Expect(mk("s-ref", &v1alpha1.OCISource{ImagePolicyRef: ref})).To(Succeed())
		})

		It("requires a source member", func() {
			Expect(mk("s-nosrc", nil)).NotTo(Succeed())
		})

		It("defaults interval and retention and enforces min interval", func() {
			Expect(mk("s-defaults", &v1alpha1.OCISource{Image: "a/b"})).To(Succeed())
			got := &v1alpha1.ReleaseSource{}
			Expect(k8sClient.Get(ctx, clientKey(&v1alpha1.ReleaseSource{ObjectMeta: metav1.ObjectMeta{Name: "s-defaults", Namespace: "default"}}), got)).To(Succeed())
			Expect(got.Spec.Interval.Duration.String()).To(Equal("1m0s"))
			Expect(*got.Spec.Retention.Days).To(BeEquivalentTo(7))
			Expect(*got.Spec.Retention.MinCount).To(BeEquivalentTo(30))

			short := &v1alpha1.ReleaseSource{
				ObjectMeta: metav1.ObjectMeta{Name: "s-short", Namespace: "default"},
				Spec: v1alpha1.ReleaseSourceSpec{
					OCI:      &v1alpha1.OCISource{Image: "a/b"},
					Interval: &metav1.Duration{Duration: 5 * 1e9},
				},
			}
			Expect(k8sClient.Create(ctx, short)).NotTo(Succeed())
		})

		It("rejects names over 230 characters", func() {
			Expect(mk(strings.Repeat("a", 231), &v1alpha1.OCISource{Image: "a/b"})).NotTo(Succeed())
		})
	})
})

func clientKey(o metav1.Object) types.NamespacedName {
	return types.NamespacedName{Namespace: o.GetNamespace(), Name: o.GetName()}
}
