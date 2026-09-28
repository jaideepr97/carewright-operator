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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"
)

var (
	routeGVK   = schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"}
	sandboxGVK = schema.GroupVersionKind{Group: "agents.x-k8s.io", Version: "v1beta1", Kind: "Sandbox"}
)

type uiExposureSummary struct {
	Created bool
	URL     string
}

// reconcileUIExposure creates a stable cluster Service in front of the UI
// sandbox pod and an edge-terminated OpenShift Route in front of that Service.
// OpenShell owns the Sandbox and pod; the aggregate pipeline resource owns the
// Service and Route.
func reconcileUIExposure(
	ctx context.Context,
	k8sClient client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
) (uiExposureSummary, error) {
	request := &appsv1alpha1.SandboxRequest{}
	requestKey := client.ObjectKey{
		Name:      componentRequestName(owner.GetName(), "ui"),
		Namespace: owner.GetNamespace(),
	}
	if err := k8sClient.Get(ctx, requestKey, request); err != nil {
		if apierrors.IsNotFound(err) {
			return uiExposureSummary{}, nil
		}
		return uiExposureSummary{}, fmt.Errorf("get UI SandboxRequest: %w", err)
	}
	if request.Status.SandboxName == "" {
		return uiExposureSummary{}, nil
	}

	sandbox := sandboxObject()
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: request.Status.SandboxName, Namespace: owner.GetNamespace()}, sandbox); err != nil {
		if apierrors.IsNotFound(err) {
			return uiExposureSummary{}, nil
		}
		return uiExposureSummary{}, fmt.Errorf("get UI Sandbox %s: %w", request.Status.SandboxName, err)
	}
	selectorText, _, err := unstructured.NestedString(sandbox.Object, "status", "selector")
	if err != nil {
		return uiExposureSummary{}, fmt.Errorf("read UI Sandbox selector: %w", err)
	}
	if selectorText == "" {
		return uiExposureSummary{}, nil
	}
	labelSelector, err := metav1.ParseToLabelSelector(selectorText)
	if err != nil {
		return uiExposureSummary{}, fmt.Errorf("parse UI Sandbox selector %q: %w", selectorText, err)
	}
	selector, err := metav1.LabelSelectorAsMap(labelSelector)
	if err != nil {
		return uiExposureSummary{}, fmt.Errorf("convert UI Sandbox selector %q: %w", selectorText, err)
	}

	targetPort := sandboxServiceTargetPort(request.Status.Services, "http")
	if targetPort == 0 {
		targetPort = sandboxServiceSpecTargetPort(request.Spec.Services, "http")
	}
	if targetPort == 0 {
		return uiExposureSummary{}, nil
	}

	name := dependencyResourceName(owner.GetName(), "ui", 63)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: owner.GetNamespace()}}
	_, err = controllerutil.CreateOrUpdate(ctx, k8sClient, service, func() error {
		if service.ResourceVersion != "" && !metav1.IsControlledBy(service, owner) {
			return fmt.Errorf("Service %s/%s already exists and is not managed by CareWright", service.Namespace, service.Name)
		}
		setPipelineResourceLabels(service, owner, "ui")
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Selector = selector
		service.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Protocol:   corev1.ProtocolTCP,
			Port:       80,
			TargetPort: intstr.FromInt32(targetPort),
		}}
		return controllerutil.SetControllerReference(owner, service, scheme)
	})
	if err != nil {
		return uiExposureSummary{}, fmt.Errorf("reconcile UI Service: %w", err)
	}

	route := routeObject()
	route.SetName(name)
	route.SetNamespace(owner.GetNamespace())
	_, err = controllerutil.CreateOrUpdate(ctx, k8sClient, route, func() error {
		if route.GetResourceVersion() != "" && !metav1.IsControlledBy(route, owner) {
			return fmt.Errorf("Route %s/%s already exists and is not managed by CareWright", route.GetNamespace(), route.GetName())
		}
		setPipelineResourceLabels(route, owner, "ui")
		route.Object["spec"] = map[string]any{
			"to": map[string]any{
				"kind":   "Service",
				"name":   service.Name,
				"weight": int64(100),
			},
			"port": map[string]any{"targetPort": "http"},
			"tls": map[string]any{
				"termination":                   "edge",
				"insecureEdgeTerminationPolicy": "Redirect",
			},
			"wildcardPolicy": "None",
		}
		return controllerutil.SetControllerReference(owner, route, scheme)
	})
	if err != nil {
		return uiExposureSummary{}, fmt.Errorf("reconcile UI Route: %w", err)
	}

	return uiExposureSummary{Created: true, URL: routeURL(route)}, nil
}

func sandboxObject() *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(sandboxGVK)
	return object
}

func routeObject() *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(routeGVK)
	return object
}

func sandboxServiceTargetPort(services []appsv1alpha1.SandboxServiceStatus, name string) int32 {
	for _, service := range services {
		if service.Name == name {
			return service.TargetPort
		}
	}
	return 0
}

func sandboxServiceSpecTargetPort(services []appsv1alpha1.SandboxServiceSpec, name string) int32 {
	for _, service := range services {
		if service.Name == name {
			return service.TargetPort
		}
	}
	return 0
}

func setPipelineResourceLabels(object client.Object, owner client.Object, component string) {
	labels := object.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[managedByLabel] = managedByValue
	labels[pipelineLabel] = string(owner.GetUID())
	labels[componentLabel] = component
	object.SetLabels(labels)
}

func routeURL(route *unstructured.Unstructured) string {
	ingress, _, _ := unstructured.NestedSlice(route.Object, "status", "ingress")
	for _, item := range ingress {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		host, _, _ := unstructured.NestedString(entry, "host")
		if host != "" {
			return "https://" + host
		}
	}
	return ""
}

func setUIExposureCondition(conditions *[]metav1.Condition, generation int64, exposure uiExposureSummary) {
	condition := metav1.Condition{
		Type:               "Exposed",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             "UISandboxPending",
		Message:            "Waiting for the UI sandbox selector before creating its Service and Route",
	}
	if exposure.Created {
		condition.Reason = "RoutePending"
		condition.Message = "The UI Service and Route exist; waiting for OpenShift to assign a route host"
	}
	if exposure.URL != "" {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "RouteReady"
		condition.Message = fmt.Sprintf("The UI is available at %s", exposure.URL)
	}
	meta.SetStatusCondition(conditions, condition)
}
