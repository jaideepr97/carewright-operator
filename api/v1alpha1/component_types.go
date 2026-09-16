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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ComponentSpec describes the runtime shared by every pipeline component.
type ComponentSpec struct {
	// Image is the complete container image reference, including its registry and tag or digest.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// ExtraProviders attaches additional OpenShell credential providers to this component.
	// Domain-specific providers should be configured on their corresponding integration.
	// +optional
	ExtraProviders []string `json:"extraProviders,omitempty"`

	// ExtraEnv contains non-secret environment variables that do not yet have first-class fields.
	// +optional
	ExtraEnv map[string]string `json:"extraEnv,omitempty"`
}

// PythonComponentSpec describes Python runtime settings shared by service components.
type PythonComponentSpec struct {
	ComponentSpec `json:",inline"`

	// PythonPath is translated to PYTHONPATH.
	// +optional
	PythonPath string `json:"pythonPath,omitempty"`
}

// ArtifactStoreSpec configures the object store shared by pipeline components.
type ArtifactStoreSpec struct {
	// URL is the artifact store endpoint.
	// +optional
	URL string `json:"url,omitempty"`

	// ArtifactBucket contains non-PHI pipeline artifacts.
	// +kubebuilder:default="cpg-artifacts"
	// +optional
	ArtifactBucket string `json:"artifactBucket,omitempty"`

	// PHIBucket contains artifacts with protected health information.
	// +optional
	PHIBucket string `json:"phiBucket,omitempty"`

	// CredentialsProvider is an OpenShell provider that supplies
	// ARTIFACT_STORE_ACCESS_KEY and ARTIFACT_STORE_SECRET_KEY.
	// +optional
	CredentialsProvider string `json:"credentialsProvider,omitempty"`
}

// ObservabilitySpec configures tracing shared by pipeline components.
type ObservabilitySpec struct {
	// MLflowTrackingURI is translated to MLFLOW_TRACKING_URI.
	// +optional
	MLflowTrackingURI string `json:"mlflowTrackingURI,omitempty"`
}

// LLMConfigSpec configures the model endpoint shared by LLM-enabled components.
type LLMConfigSpec struct {
	// URL is the LiteLLM-compatible endpoint.
	// +optional
	URL string `json:"url,omitempty"`

	// Model is the model identifier sent to the endpoint.
	// +kubebuilder:default="default"
	// +optional
	Model string `json:"model,omitempty"`

	// CredentialsProvider is an OpenShell provider that supplies LLM_API_KEY.
	// +optional
	CredentialsProvider string `json:"credentialsProvider,omitempty"`

	// RequestTimeoutSeconds is the maximum duration of an LLM request.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=1
	// +optional
	RequestTimeoutSeconds int32 `json:"requestTimeoutSeconds,omitempty"`
}

// PipelineReference identifies another pipeline resource in the same namespace.
type PipelineReference struct {
	// Name is the metadata.name of the referenced pipeline resource.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// WorkloadStatus captures status shared by the aggregate application resources.
type WorkloadStatus struct {
	// ObservedGeneration is the most recent generation reconciled by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions summarize the current availability and reconciliation state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Endpoints contains operator-published addresses for pipeline entry points.
	// Internal component wiring is intentionally not exposed as desired state.
	// +optional
	Endpoints map[string]string `json:"endpoints,omitempty"`
}
