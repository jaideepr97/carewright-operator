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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Pipeline UI exposure", func() {
	It("creates a stable Service and edge-terminated Route for the UI sandbox", func() {
		testScheme := runtime.NewScheme()
		Expect(corev1.AddToScheme(testScheme)).To(Succeed())
		Expect(appsv1alpha1.AddToScheme(testScheme)).To(Succeed())

		owner := &appsv1alpha1.CPGIngester{ObjectMeta: metav1.ObjectMeta{
			Name: "example", Namespace: "default", UID: types.UID("pipeline-uid"),
		}}
		request := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: "example-ui", Namespace: "default"},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Services: []appsv1alpha1.SandboxServiceSpec{{Name: "http", TargetPort: 8080}},
			},
			Status: appsv1alpha1.SandboxRequestStatus{
				SandboxName: "example-ui",
				Services:    []appsv1alpha1.SandboxServiceStatus{{Name: "http", TargetPort: 8080}},
			},
		}
		sandbox := sandboxObject()
		sandbox.SetName("example-ui")
		sandbox.SetNamespace("default")
		sandbox.Object["status"] = map[string]any{
			"selector": "agents.x-k8s.io/sandbox-name-hash=abc123",
		}

		fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(owner, request, sandbox).Build()
		exposure, err := reconcileUIExposure(context.Background(), fakeClient, testScheme, owner)
		Expect(err).NotTo(HaveOccurred())
		Expect(exposure.Created).To(BeTrue())
		Expect(exposure.URL).To(BeEmpty())

		service := &corev1.Service{}
		key := client.ObjectKey{Name: "example-ui", Namespace: "default"}
		Expect(fakeClient.Get(context.Background(), key, service)).To(Succeed())
		Expect(service.Spec.Selector).To(Equal(map[string]string{
			"agents.x-k8s.io/sandbox-name-hash": "abc123",
		}))
		Expect(service.Spec.Ports).To(HaveLen(1))
		Expect(service.Spec.Ports[0].Port).To(Equal(int32(80)))
		Expect(service.Spec.Ports[0].TargetPort.IntVal).To(Equal(int32(8080)))
		Expect(metav1.IsControlledBy(service, owner)).To(BeTrue())

		route := routeObject()
		Expect(fakeClient.Get(context.Background(), key, route)).To(Succeed())
		serviceName, _, err := unstructured.NestedString(route.Object, "spec", "to", "name")
		Expect(err).NotTo(HaveOccurred())
		Expect(serviceName).To(Equal("example-ui"))
		termination, _, err := unstructured.NestedString(route.Object, "spec", "tls", "termination")
		Expect(err).NotTo(HaveOccurred())
		Expect(termination).To(Equal("edge"))
		Expect(metav1.IsControlledBy(route, owner)).To(BeTrue())

		route.Object["status"] = map[string]any{
			"ingress": []any{map[string]any{"host": "example-ui.apps.test.example"}},
		}
		Expect(fakeClient.Update(context.Background(), route)).To(Succeed())
		exposure, err = reconcileUIExposure(context.Background(), fakeClient, testScheme, owner)
		Expect(err).NotTo(HaveOccurred())
		Expect(exposure.URL).To(Equal("https://example-ui.apps.test.example"))
	})
})
