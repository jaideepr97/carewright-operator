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

// CarePlanWriterSpec defines the desired state of CarePlanWriter.
type CarePlanWriterSpec struct {
	// PatientData scans and normalizes patient data for the workflow.
	PatientData ComponentSpec `json:"patientData"`

	// LLMReasoning resolves guidelines, evaluates decisions, and composes plans.
	LLMReasoning ComponentSpec `json:"llmReasoning"`

	// DecisionEngine provides the Python-facing wrapper around the Kogito runtime.
	DecisionEngine ComponentSpec `json:"decisionEngine"`

	// FHIRGeneration generates and reviews FHIR bundles.
	FHIRGeneration ComponentSpec `json:"fhirGeneration"`

	// FHIRServer writes approved care plans to the configured FHIR server.
	FHIRServer ComponentSpec `json:"fhirServer"`

	// BFF exposes the backend API used by the Care Plan Writer UI.
	BFF ComponentSpec `json:"bff"`

	// UI serves the Care Plan Writer web application.
	UI ComponentSpec `json:"ui"`

	// MCP exposes the Care Plan Writer tools through the Model Context Protocol.
	MCP ComponentSpec `json:"mcp"`

	// DecisionService runs the Kogito decision service used to evaluate DMN.
	DecisionService ComponentSpec `json:"decisionService"`

	// Workflow configures the platform-managed Care Plan Writer SonataFlow.
	// +optional
	Workflow *WorkflowSpec `json:"workflow,omitempty"`
}

// CarePlanWriterStatus defines the observed state of CarePlanWriter.
type CarePlanWriterStatus struct {
	WorkloadStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CarePlanWriter is the Schema for the careplanwriters API.
type CarePlanWriter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CarePlanWriterSpec   `json:"spec,omitempty"`
	Status CarePlanWriterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CarePlanWriterList contains a list of CarePlanWriter.
type CarePlanWriterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CarePlanWriter `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CarePlanWriter{}, &CarePlanWriterList{})
}
