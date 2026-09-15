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

	appsv1alpha1 "cpgtoacp.io/cpgtoacp-operator/api/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

var _ = Describe("CarePlanWriter Controller", func() {
	const resourceName = "careplanwriter-controller-test"
	resourceKey := types.NamespacedName{Name: resourceName, Namespace: "default"}

	AfterEach(func() {
		resource := &appsv1alpha1.CarePlanWriter{}
		if err := k8sClient.Get(ctx, resourceKey, resource); err == nil {
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		}
	})

	It("records that the current specification was accepted", func() {
		image := func() appsv1alpha1.ComponentSpec {
			return appsv1alpha1.ComponentSpec{Image: "example.invalid/test:latest"}
		}
		resource := &appsv1alpha1.CarePlanWriter{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
			Spec: appsv1alpha1.CarePlanWriterSpec{
				PatientData:     image(),
				LLMReasoning:    image(),
				DecisionEngine:  image(),
				FHIRGeneration:  image(),
				FHIRServer:      image(),
				BFF:             image(),
				UI:              image(),
				MCP:             image(),
				DecisionService: image(),
			},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		reconciler := &CarePlanWriterReconciler{Client: k8sClient}
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: resourceKey})
		Expect(err).NotTo(HaveOccurred())

		updated := &appsv1alpha1.CarePlanWriter{}
		Expect(k8sClient.Get(ctx, resourceKey, updated)).To(Succeed())
		Expect(updated.Status.ObservedGeneration).To(Equal(updated.Generation))
		Expect(updated.Status.Conditions).To(ContainElement(And(
			HaveField("Type", "Accepted"),
			HaveField("Status", metav1.ConditionTrue),
			HaveField("Reason", "SpecAccepted"),
		)))
	})
})
