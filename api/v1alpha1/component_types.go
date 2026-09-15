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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ComponentSpec describes the container image and environment for one application component.
// Env uses the native Kubernetes EnvVar shape so values can come from Secrets and ConfigMaps.
type ComponentSpec struct {
	// Image is the complete container image reference, including its registry and tag or digest.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Env is the complete environment passed to the component container.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// WorkflowSpec describes environment variables passed to the platform-managed SonataFlow pod.
type WorkflowSpec struct {
	// Env is the complete environment passed to the workflow container.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
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
}
