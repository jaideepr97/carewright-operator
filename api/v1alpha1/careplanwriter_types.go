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

// EmbeddingSpec configures vector embedding generation for guideline retrieval.
type EmbeddingSpec struct {
	// Provider selects the embedding implementation, for example openai or local.
	// +optional
	Provider string `json:"provider,omitempty"`

	// Model is the embedding model identifier.
	// +optional
	Model string `json:"model,omitempty"`

	// URL overrides the embedding API endpoint. The LLM URL is used when omitted.
	// +optional
	URL string `json:"url,omitempty"`

	// CredentialsProvider is an OpenShell provider that supplies EMBEDDING_API_KEY.
	// The LLM credentials provider is used when omitted.
	// +optional
	CredentialsProvider string `json:"credentialsProvider,omitempty"`
}

// AITransparencySpec configures the provenance recorded with generated care plans.
type AITransparencySpec struct {
	// CapturePrompts emits rendered prompts as FHIR DocumentReferences.
	// Prompts can contain PHI and should be disabled where retention is inappropriate.
	// +kubebuilder:default=true
	// +optional
	CapturePrompts bool `json:"capturePrompts,omitempty"`

	// ModelCardURL is recorded as the model-card DocumentReference.
	// +optional
	ModelCardURL string `json:"modelCardURL,omitempty"`

	// Reviewer configures the default human verifier.
	// +optional
	Reviewer *ReviewerSpec `json:"reviewer,omitempty"`
}

// ReviewerSpec identifies the default human verifier of a care plan.
type ReviewerSpec struct {
	// Display is the human-readable reviewer name.
	// +optional
	Display string `json:"display,omitempty"`

	// Reference is the reviewer FHIR reference, for example Practitioner/123.
	// +optional
	Reference string `json:"reference,omitempty"`

	// IdentifierSystem is the URI of the reviewer's identifier system.
	// +optional
	IdentifierSystem string `json:"identifierSystem,omitempty"`

	// IdentifierValue is the reviewer identifier in IdentifierSystem.
	// +optional
	IdentifierValue string `json:"identifierValue,omitempty"`
}

// FHIRTargetSpec configures the target EHR written by the FHIR server component.
type FHIRTargetSpec struct {
	// URL is the base FHIR R4 endpoint.
	// +optional
	URL string `json:"url,omitempty"`

	// CredentialsProvider is an optional OpenShell provider that supplies
	// FHIR_CLIENT_ID and FHIR_CLIENT_SECRET.
	// +optional
	CredentialsProvider string `json:"credentialsProvider,omitempty"`
}

// CarePlanLLMReasoningComponentSpec configures guideline reasoning.
type CarePlanLLMReasoningComponentSpec struct {
	PythonComponentSpec `json:",inline"`

	// Embedding configures guideline vector embeddings.
	// +optional
	Embedding *EmbeddingSpec `json:"embedding,omitempty"`
}

// DecisionServiceComponentSpec configures the Kogito decision service runtime.
type DecisionServiceComponentSpec struct {
	ComponentSpec `json:",inline"`

	// JavaOptions contains additional JVM options.
	// +optional
	JavaOptions string `json:"javaOptions,omitempty"`
}

// CarePlanWriterSpec defines the desired state of CarePlanWriter.
type CarePlanWriterSpec struct {
	// Sandbox configures OpenShell settings shared by all writer components.
	// +optional
	Sandbox PipelineSandboxSpec `json:"sandbox,omitempty"`

	// ArtifactStore configures shared artifact and PHI storage.
	// +optional
	ArtifactStore *ArtifactStoreSpec `json:"artifactStore,omitempty"`

	// Observability configures shared tracing.
	// +optional
	Observability *ObservabilitySpec `json:"observability,omitempty"`

	// LLM configures the model endpoint used by reasoning and FHIR generation.
	// +optional
	LLM *LLMConfigSpec `json:"llm,omitempty"`

	// FHIRTarget configures the target EHR.
	// +optional
	FHIRTarget *FHIRTargetSpec `json:"fhirTarget,omitempty"`

	// AITransparency configures prompt provenance, the model card, and the default reviewer.
	// +optional
	AITransparency *AITransparencySpec `json:"aiTransparency,omitempty"`

	// PatientData scans and normalizes patient data for the workflow.
	PatientData PythonComponentSpec `json:"patientData"`

	// LLMReasoning resolves guidelines, evaluates decisions, and composes plans.
	LLMReasoning CarePlanLLMReasoningComponentSpec `json:"llmReasoning"`

	// DecisionEngine provides the Python-facing wrapper around the Kogito runtime.
	DecisionEngine PythonComponentSpec `json:"decisionEngine"`

	// FHIRGeneration generates and reviews FHIR bundles.
	FHIRGeneration PythonComponentSpec `json:"fhirGeneration"`

	// FHIRServer writes approved care plans to the configured FHIR server.
	FHIRServer PythonComponentSpec `json:"fhirServer"`

	// BFF exposes the backend API used by the Care Plan Writer UI.
	BFF PythonComponentSpec `json:"bff"`

	// UI serves the Care Plan Writer web application.
	UI ComponentSpec `json:"ui"`

	// MCP exposes the Care Plan Writer tools through the Model Context Protocol.
	MCP PythonComponentSpec `json:"mcp"`

	// DecisionService runs the Kogito decision service used to evaluate DMN.
	DecisionService DecisionServiceComponentSpec `json:"decisionService"`
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
