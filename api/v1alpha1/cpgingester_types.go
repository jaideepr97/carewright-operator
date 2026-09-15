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

// CPGIngesterSpec defines the desired state of CPGIngester.
type CPGIngesterSpec struct {
	// Ingestion parses source CPG documents and performs OCR when required.
	Ingestion ComponentSpec `json:"ingestion"`

	// LLMAnalysis extracts decision logic and recommendations with an LLM.
	LLMAnalysis ComponentSpec `json:"llmAnalysis"`

	// Assembly assembles generated artifacts into the published bundle.
	Assembly ComponentSpec `json:"assembly"`

	// Delivery publishes assembled artifacts to the artifact store.
	Delivery ComponentSpec `json:"delivery"`

	// BFF exposes the backend API used by the CPG Ingester UI.
	BFF ComponentSpec `json:"bff"`

	// UI serves the CPG Ingester web application.
	UI ComponentSpec `json:"ui"`

	// Workflow configures the platform-managed CPG Ingester SonataFlow.
	// +optional
	Workflow *WorkflowSpec `json:"workflow,omitempty"`
}

// CPGIngesterStatus defines the observed state of CPGIngester.
type CPGIngesterStatus struct {
	WorkloadStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CPGIngester is the Schema for the cpgingesters API.
type CPGIngester struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CPGIngesterSpec   `json:"spec,omitempty"`
	Status CPGIngesterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CPGIngesterList contains a list of CPGIngester.
type CPGIngesterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CPGIngester `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CPGIngester{}, &CPGIngesterList{})
}
