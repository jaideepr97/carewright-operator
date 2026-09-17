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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "cpgtoacp.io/cpgtoacp-operator/api/v1alpha1"
)

const (
	managedByLabel = "apps.cpgtoacp.io/managed-by"
	pipelineLabel  = "apps.cpgtoacp.io/pipeline-uid"
	componentLabel = "apps.cpgtoacp.io/component"
	managedByValue = "pipeline-controller"
)

type sandboxComponent struct {
	Name      string
	Spec      appsv1alpha1.ComponentSpec
	Command   []string
	Env       map[string]string
	Providers []string
}

type sandboxSummary struct {
	Desired int
	Ready   int
}

func reconcileComponentSandboxes(
	ctx context.Context,
	k8sClient client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	config appsv1alpha1.PipelineSandboxSpec,
	components []sandboxComponent,
) (sandboxSummary, error) {
	desired := make(map[string]struct{}, len(components))
	summary := sandboxSummary{Desired: len(components)}

	for _, component := range components {
		requestName := componentRequestName(owner.GetName(), component.Name)
		desired[requestName] = struct{}{}
		request := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: requestName, Namespace: owner.GetNamespace()},
		}
		_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, request, func() error {
			if request.Labels == nil {
				request.Labels = map[string]string{}
			}
			request.Labels[managedByLabel] = managedByValue
			request.Labels[pipelineLabel] = string(owner.GetUID())
			request.Labels[componentLabel] = component.Name

			command := component.Command
			if len(component.Spec.Command) != 0 {
				command = component.Spec.Command
			}
			providers := append(slices.Clone(component.Providers), component.Spec.ExtraProviders...)
			request.Spec = appsv1alpha1.SandboxRequestSpec{
				SandboxName: componentSandboxName(owner.GetName(), component.Name),
				Gateway:     config.Gateway,
				Workspace:   config.Workspace,
				Image:       component.Spec.Image,
				Command:     slices.Clone(command),
				PolicyRef:   component.Spec.PolicyRef.DeepCopy(),
				Providers:   uniqueStrings(providers),
				Env:         mergedEnv(component.Spec.ExtraEnv, component.Env),
				Resources:   component.Spec.Resources,
				Labels: map[string]string{
					"app.kubernetes.io/component":  component.Name,
					"app.kubernetes.io/instance":   safeLabelValue(owner.GetName()),
					"app.kubernetes.io/managed-by": "cpgtoacp-operator",
				},
				ApprovalMode: config.ApprovalMode,
			}
			return controllerutil.SetControllerReference(owner, request, scheme)
		})
		if err != nil {
			return summary, fmt.Errorf("reconcile %s SandboxRequest: %w", component.Name, err)
		}
		if request.DeletionTimestamp.IsZero() && meta.IsStatusConditionTrue(request.Status.Conditions, readyCondition) {
			summary.Ready++
		}
	}

	var existing appsv1alpha1.SandboxRequestList
	if err := k8sClient.List(ctx, &existing,
		client.InNamespace(owner.GetNamespace()),
		client.MatchingLabels{managedByLabel: managedByValue, pipelineLabel: string(owner.GetUID())},
	); err != nil {
		return summary, fmt.Errorf("list owned SandboxRequests: %w", err)
	}
	for index := range existing.Items {
		request := &existing.Items[index]
		if _, keep := desired[request.Name]; keep || !metav1.IsControlledBy(request, owner) {
			continue
		}
		if err := k8sClient.Delete(ctx, request); client.IgnoreNotFound(err) != nil {
			return summary, fmt.Errorf("delete stale SandboxRequest %s: %w", request.Name, err)
		}
	}

	return summary, nil
}

func componentRequestName(ownerName, component string) string {
	name := ownerName + "-" + component
	if len(name) <= 253 && len(validation.IsDNS1123Subdomain(name)) == 0 {
		return name
	}
	hash := shortHash(name)
	maxPrefix := 253 - len(hash) - 1
	prefix := strings.TrimRight(name[:maxPrefix], "-.")
	return prefix + "-" + hash
}

func componentSandboxName(ownerName, component string) string {
	const maxSandboxName = 19
	candidate := ownerName + "-" + component
	if len(candidate) <= maxSandboxName {
		return candidate
	}
	hash := shortHash(candidate)
	maxPrefix := maxSandboxName - len(hash) - 1
	prefix := component
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	prefix = strings.Trim(prefix, "-")
	return prefix + "-" + hash
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:8]
}

func safeLabelValue(value string) string {
	if len(value) <= 63 && len(validation.IsValidLabelValue(value)) == 0 {
		return value
	}
	hash := shortHash(value)
	maxPrefix := 63 - len(hash) - 1
	prefix := strings.Trim(value[:maxPrefix], "-_.")
	return prefix + "-" + hash
}

func mergedEnv(extra, generated map[string]string) map[string]string {
	result := maps.Clone(extra)
	if result == nil {
		result = map[string]string{}
	}
	maps.Copy(result, generated)
	return result
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func setIfNotEmpty(env map[string]string, key, value string) {
	if value != "" {
		env[key] = value
	}
}

func artifactStoreInputs(spec *appsv1alpha1.ArtifactStoreSpec) (map[string]string, string) {
	env := map[string]string{}
	if spec == nil {
		return env, ""
	}
	setIfNotEmpty(env, "ARTIFACT_STORE_URL", spec.URL)
	setIfNotEmpty(env, "ARTIFACT_STORE_BUCKET", spec.ArtifactBucket)
	setIfNotEmpty(env, "PHI_STORE_BUCKET", spec.PHIBucket)
	return env, spec.CredentialsProvider
}

func observabilityInputs(spec *appsv1alpha1.ObservabilitySpec) map[string]string {
	env := map[string]string{}
	if spec != nil {
		setIfNotEmpty(env, "MLFLOW_TRACKING_URI", spec.MLflowTrackingURI)
	}
	return env
}

func llmInputs(spec *appsv1alpha1.LLMConfigSpec) (map[string]string, string) {
	env := map[string]string{}
	if spec == nil {
		return env, ""
	}
	setIfNotEmpty(env, "LITELLM_URL", spec.URL)
	setIfNotEmpty(env, "LLM_MODEL", spec.Model)
	if spec.RequestTimeoutSeconds > 0 {
		env["LLM_REQUEST_TIMEOUT"] = strconv.FormatInt(int64(spec.RequestTimeoutSeconds), 10)
	}
	return env, spec.CredentialsProvider
}

func pythonServiceCommand(application, port string) []string {
	return []string{"uvicorn", application, "--host", "0.0.0.0", "--port", port}
}

func boolInt(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func setSandboxReadyCondition(conditions *[]metav1.Condition, generation int64, summary sandboxSummary) {
	condition := metav1.Condition{
		Type:               readyCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             "SandboxesPending",
		Message:            fmt.Sprintf("%d of %d component sandboxes are ready", summary.Ready, summary.Desired),
	}
	if summary.Ready == summary.Desired {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "SandboxesReady"
		condition.Message = fmt.Sprintf("All %d component sandboxes are ready", summary.Desired)
	}
	meta.SetStatusCondition(conditions, condition)
}
