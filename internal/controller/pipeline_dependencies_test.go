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

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("Pipeline dependencies", func() {
	const storeName = "managed-artifacts"

	AfterEach(func() {
		store := objectStoreObject()
		store.SetName(storeName)
		store.SetNamespace("default")
		_ = k8sClient.Delete(ctx, store)
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: storeName + "-configuration", Namespace: "default"}})
	})

	It("creates a MinIO ObjectStore and configuration Secret", func() {
		owner := &appsv1alpha1.CPGIngester{ObjectMeta: metav1.ObjectMeta{Name: "managed-store-test", Namespace: "default"}}
		requested := &appsv1alpha1.ArtifactStoreSpec{
			ArtifactBucket: "artifacts",
			Managed: &appsv1alpha1.ManagedObjectStoreSpec{
				Name:             storeName,
				StorageSize:      resource.MustParse("7Gi"),
				StorageClassName: "fast",
				Servers:          2,
				VolumesPerServer: 3,
			},
		}

		summary, err := reconcileArtifactStore(ctx, k8sClient, owner, requested)
		Expect(err).NotTo(HaveOccurred())
		Expect(summary.URL).To(Equal("http://managed-artifacts-api.default.svc.cluster.local:9000"))
		Expect(summary.Spec.URL).To(Equal(summary.URL))

		configuration := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: storeName + "-configuration", Namespace: "default"}, configuration)).To(Succeed())
		Expect(string(configuration.Data["config.env"])).To(ContainSubstring("MINIO_ROOT_PASSWORD="))

		store := objectStoreObject()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: storeName, Namespace: "default"}, store)).To(Succeed())
		serviceName, found, err := unstructured.NestedString(store.Object, "spec", "services", "minio", "name")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(serviceName).To(Equal("managed-artifacts-api"))
		pools, found, err := unstructured.NestedSlice(store.Object, "spec", "pools")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(pools[0].(map[string]any)["servers"]).To(BeEquivalentTo(2))
	})
})
