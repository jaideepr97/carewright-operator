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
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"
)

var sonataFlowGVK = schema.GroupVersionKind{Group: "sonataflow.org", Version: "v1alpha08", Kind: "SonataFlow"}

//go:embed workflows/cpg-ingester-workflow.yaml
var cpgIngesterWorkflowYAML string

//go:embed workflows/cpg-ingester-props.yaml
var cpgIngesterPropsYAML string

//go:embed workflows/care-plan-writer-workflow.yaml
var carePlanWriterWorkflowYAML string

//go:embed workflows/care-plan-writer-props.yaml
var carePlanWriterPropsYAML string

type pipelineWorkflowTemplate struct {
	WorkflowYAML        string
	PropsYAML           string
	Replacements        map[string]string
	LiteralReplacements map[string]string
}

type workflowSummary struct {
	Created bool
	Ready   bool
	Name    string
	URL     string
}

func reconcilePipelineWorkflow(
	ctx context.Context,
	k8sClient client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	template pipelineWorkflowTemplate,
	endpoints map[string]string,
	desiredEndpointCount int,
) (workflowSummary, error) {
	name := workflowResourceName(owner)
	summary := workflowSummary{Name: name}
	if len(endpoints) != desiredEndpointCount {
		return summary, nil
	}

	replacements := maps.Clone(template.Replacements)
	for placeholder, component := range replacements {
		endpoint := endpoints[component]
		if endpoint == "" {
			return summary, fmt.Errorf("component %q has no gateway service URL", component)
		}
		replacements[placeholder] = strings.TrimRight(endpoint, "/")
	}
	replacements[templateWorkflowURL(template.WorkflowYAML)] = workflowServiceURL(owner)
	for placeholder, replacement := range template.LiteralReplacements {
		replacements[placeholder] = replacement
	}

	workflowYAML := template.WorkflowYAML
	for placeholder, replacement := range replacements {
		workflowYAML = strings.ReplaceAll(workflowYAML, placeholder, replacement)
	}
	desired, err := decodeUnstructured(workflowYAML)
	if err != nil {
		return summary, fmt.Errorf("decode SonataFlow template: %w", err)
	}
	desired.SetName(name)
	desired.SetNamespace(owner.GetNamespace())
	desired.SetGroupVersionKind(sonataFlowGVK)
	workflow := sonataFlowObject()
	workflow.SetName(name)
	workflow.SetNamespace(owner.GetNamespace())
	_, err = controllerutil.CreateOrUpdate(ctx, k8sClient, workflow, func() error {
		workflow.SetLabels(mergedStringMap(workflow.GetLabels(), desired.GetLabels(), map[string]string{
			managedByLabel: managedByValue,
			pipelineLabel:  string(owner.GetUID()),
		}))
		workflow.SetAnnotations(maps.Clone(desired.GetAnnotations()))
		workflow.Object["spec"] = runtime.DeepCopyJSONValue(desired.Object["spec"])
		return controllerutil.SetControllerReference(owner, workflow, scheme)
	})
	if err != nil {
		return summary, fmt.Errorf("reconcile SonataFlow %s: %w", name, err)
	}

	if err := reconcileWorkflowProperties(ctx, k8sClient, scheme, owner, name, template.PropsYAML); err != nil {
		return summary, err
	}

	summary.Created = true
	summary.Ready = sonataFlowReady(workflow)
	summary.URL, _, _ = unstructured.NestedString(workflow.Object, "status", "address", "url")
	if summary.URL == "" {
		summary.URL = workflowServiceURL(owner)
	}
	return summary, nil
}

func reconcileWorkflowProperties(ctx context.Context, k8sClient client.Client, scheme *runtime.Scheme, owner client.Object, workflowName, data string) error {
	var desired corev1.ConfigMap
	if err := yaml.Unmarshal([]byte(data), &desired); err != nil {
		return fmt.Errorf("decode SonataFlow properties: %w", err)
	}
	properties := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: workflowName + "-props", Namespace: owner.GetNamespace()}}
	_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, properties, func() error {
		properties.Labels = mergedStringMap(properties.Labels, desired.Labels, map[string]string{
			managedByLabel: managedByValue,
			pipelineLabel:  string(owner.GetUID()),
		})
		properties.Data = maps.Clone(desired.Data)
		return controllerutil.SetControllerReference(owner, properties, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconcile SonataFlow properties %s: %w", properties.Name, err)
	}
	return nil
}

func decodeUnstructured(data string) (*unstructured.Unstructured, error) {
	jsonData, err := yaml.YAMLToJSON([]byte(data))
	if err != nil {
		return nil, err
	}
	object := map[string]any{}
	if err := json.Unmarshal(jsonData, &object); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: object}, nil
}

func sonataFlowObject() *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(sonataFlowGVK)
	return object
}

func sonataFlowReady(workflow *unstructured.Unstructured) bool {
	observed, _, _ := unstructured.NestedInt64(workflow.Object, "status", "observedGeneration")
	if observed != workflow.GetGeneration() {
		return false
	}
	conditions, _, _ := unstructured.NestedSlice(workflow.Object, "status", "conditions")
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if ok && condition["type"] == "Running" && condition["status"] == "True" {
			return true
		}
	}
	return false
}

func workflowResourceName(owner client.Object) string {
	candidate := owner.GetName() + "-workflow"
	// Reserve room for the required <workflow-name>-props ConfigMap.
	const maxNameLength = 57
	if len(candidate) <= maxNameLength && len(validation.IsDNS1123Label(candidate)) == 0 {
		return candidate
	}
	hash := shortHash(candidate)
	prefix := strings.Trim(candidate[:maxNameLength-len(hash)-1], "-")
	return prefix + "-" + hash
}

func workflowServiceHost(owner client.Object) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", workflowResourceName(owner), owner.GetNamespace())
}

func workflowServiceURL(owner client.Object) string {
	return "http://" + workflowServiceHost(owner) + ":80"
}

func templateWorkflowURL(workflow string) string {
	if strings.Contains(workflow, "http://cpgingester:80") {
		return "http://cpgingester:80"
	}
	return "http://acpwriter:80"
}

func mergedStringMap(values ...map[string]string) map[string]string {
	result := map[string]string{}
	for _, value := range values {
		maps.Copy(result, value)
	}
	return result
}

func setPipelineReadyCondition(conditions *[]metav1.Condition, generation int64, sandboxes sandboxSummary, workflow workflowSummary) {
	condition := metav1.Condition{
		Type:               readyCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             "SandboxesPending",
		Message:            fmt.Sprintf("%d of %d component sandboxes are ready", sandboxes.Ready, sandboxes.Desired),
	}
	if sandboxes.Ready == sandboxes.Desired && !workflow.Created {
		condition.Reason = "WorkflowPending"
		condition.Message = "Waiting for all gateway-generated component URLs before creating the SonataFlow workflow"
	}
	if sandboxes.Ready == sandboxes.Desired && workflow.Created && !workflow.Ready {
		condition.Reason = "WorkflowProgressing"
		condition.Message = fmt.Sprintf("SonataFlow workflow %q is not running yet", workflow.Name)
	}
	if sandboxes.Ready == sandboxes.Desired && workflow.Ready {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "PipelineReady"
		condition.Message = fmt.Sprintf("All %d component sandboxes and SonataFlow workflow %q are ready", sandboxes.Desired, workflow.Name)
	}
	meta.SetStatusCondition(conditions, condition)
}
