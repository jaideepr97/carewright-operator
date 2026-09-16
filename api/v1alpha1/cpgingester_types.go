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

// CPGIngestionComponentSpec configures document parsing and OCR.
type CPGIngestionComponentSpec struct {
	ComponentSpec `json:",inline"`

	// LogLevel controls application logging.
	// +optional
	LogLevel string `json:"logLevel,omitempty"`

	// DoclingLogLevel controls Docling library logging.
	// +optional
	DoclingLogLevel string `json:"doclingLogLevel,omitempty"`

	// PythonUnbuffered enables unbuffered Python output.
	// +kubebuilder:default=true
	// +optional
	PythonUnbuffered bool `json:"pythonUnbuffered,omitempty"`

	// DoclingCacheDirectory stores downloaded Docling data.
	// +optional
	DoclingCacheDirectory string `json:"doclingCacheDirectory,omitempty"`

	// DoclingArtifactsPath contains preloaded Docling models.
	// +optional
	DoclingArtifactsPath string `json:"doclingArtifactsPath,omitempty"`

	// HuggingFaceTransferEnabled enables the Hugging Face accelerated transfer client.
	// +optional
	HuggingFaceTransferEnabled bool `json:"huggingFaceTransferEnabled,omitempty"`

	// HuggingFaceOffline prevents model downloads at runtime.
	// +kubebuilder:default=true
	// +optional
	HuggingFaceOffline bool `json:"huggingFaceOffline,omitempty"`

	// OCREnabled enables the conditional OCR pass for scanned documents.
	// +kubebuilder:default=true
	// +optional
	OCREnabled bool `json:"ocrEnabled,omitempty"`
}

// CPGLLMAnalysisComponentSpec configures LLM analysis behavior.
type CPGLLMAnalysisComponentSpec struct {
	PythonComponentSpec `json:",inline"`

	// FigureInterpretationEnabled enables vision analysis of document figures.
	// +kubebuilder:default=true
	// +optional
	FigureInterpretationEnabled bool `json:"figureInterpretationEnabled,omitempty"`

	// FigureInterpretationMaxFigures limits vision calls per document.
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=0
	// +optional
	FigureInterpretationMaxFigures int32 `json:"figureInterpretationMaxFigures,omitempty"`
}

// CPGIngesterSpec defines the desired state of CPGIngester.
type CPGIngesterSpec struct {
	// ArtifactStore configures shared artifact storage.
	// +optional
	ArtifactStore *ArtifactStoreSpec `json:"artifactStore,omitempty"`

	// Observability configures shared tracing.
	// +optional
	Observability *ObservabilitySpec `json:"observability,omitempty"`

	// LLM configures the model endpoint used by LLM analysis.
	// +optional
	LLM *LLMConfigSpec `json:"llm,omitempty"`

	// CarePlanWriterRef selects the CarePlanWriter integrated with this pipeline.
	// The operator resolves its generated backend address.
	// +optional
	CarePlanWriterRef *PipelineReference `json:"carePlanWriterRef,omitempty"`

	// Ingestion parses source CPG documents and performs OCR when required.
	Ingestion CPGIngestionComponentSpec `json:"ingestion"`

	// LLMAnalysis extracts decision logic and recommendations with an LLM.
	LLMAnalysis CPGLLMAnalysisComponentSpec `json:"llmAnalysis"`

	// Assembly assembles generated artifacts into the published bundle.
	Assembly PythonComponentSpec `json:"assembly"`

	// Delivery publishes assembled artifacts to the artifact store.
	Delivery PythonComponentSpec `json:"delivery"`

	// BFF exposes the backend API used by the CPG Ingester UI.
	BFF PythonComponentSpec `json:"bff"`

	// UI serves the CPG Ingester web application.
	UI ComponentSpec `json:"ui"`
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
