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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"
)

var _ = Describe("pipeline Secret-backed providers", func() {
	source := func(name, providerType string) *appsv1alpha1.ProviderSourceSpec {
		return &appsv1alpha1.ProviderSourceSpec{
			Type:      providerType,
			SecretRef: corev1.LocalObjectReference{Name: name},
		}
	}
	component := func(components []sandboxComponent, name string) sandboxComponent {
		for _, item := range components {
			if item.Name == name {
				return item
			}
		}
		Fail("component " + name + " not found")
		return sandboxComponent{}
	}

	It("passes artifact and LLM Secret references to the right ingester components", func() {
		ingester := &appsv1alpha1.CPGIngester{
			ObjectMeta: metav1.ObjectMeta{Name: "cpg", Namespace: "carewright", UID: types.UID("ingester-uid")},
			Spec: appsv1alpha1.CPGIngesterSpec{
				ArtifactStore: &appsv1alpha1.ArtifactStoreSpec{Credentials: source("artifact-secret", "aws-s3")},
				LLM:           &appsv1alpha1.LLMConfigSpec{Credentials: source("llm-secret", "openai")},
			},
		}
		components := cpgIngesterComponents(ingester, nil, "")
		ingestion := component(components, "ingestion")
		analysis := component(components, "llm-analysis")
		ui := component(components, "ui")
		Expect(ingestion.Registrations).To(HaveLen(1))
		Expect(ingestion.Registrations[0].SecretRef.Name).To(Equal("artifact-secret"))
		Expect(ingestion.Providers).To(ContainElement(ingestion.Registrations[0].Name))
		Expect(analysis.Registrations).To(HaveLen(2))
		Expect(analysis.Registrations).To(ContainElement(appsv1alpha1.SandboxProviderRegistration{
			Name:      analysis.Registrations[1].Name,
			Type:      "openai",
			SecretRef: corev1.LocalObjectReference{Name: "llm-secret"},
		}))
		Expect(ui.Registrations).To(BeEmpty())
	})

	It("passes embedding and FHIR Secret references only to their writer consumers", func() {
		writer := &appsv1alpha1.CarePlanWriter{
			ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "carewright", UID: types.UID("writer-uid")},
			Spec: appsv1alpha1.CarePlanWriterSpec{
				ArtifactStore: &appsv1alpha1.ArtifactStoreSpec{Credentials: source("artifact-secret", "aws-s3")},
				LLM:           &appsv1alpha1.LLMConfigSpec{Credentials: source("llm-secret", "openai")},
				LLMReasoning: appsv1alpha1.CarePlanLLMReasoningComponentSpec{
					Embedding: &appsv1alpha1.EmbeddingSpec{Credentials: source("embedding-secret", "openai")},
				},
				FHIRTarget: &appsv1alpha1.FHIRTargetSpec{Credentials: source("fhir-secret", "fhir")},
			},
		}
		components := carePlanWriterComponents(writer, nil, "")
		reasoning := component(components, "llm-reasoning")
		fhir := component(components, "fhir-server")
		ui := component(components, "ui")
		Expect(reasoning.Registrations).To(HaveLen(3))
		Expect(reasoning.Registrations[2].SecretRef.Name).To(Equal("embedding-secret"))
		Expect(fhir.Registrations).To(HaveLen(2))
		Expect(fhir.Registrations[1].SecretRef.Name).To(Equal("fhir-secret"))
		Expect(ui.Registrations).To(BeEmpty())
	})
})
