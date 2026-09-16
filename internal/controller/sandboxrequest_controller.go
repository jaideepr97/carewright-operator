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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "cpgtoacp.io/cpgtoacp-operator/api/v1alpha1"
)

const (
	sandboxFinalizer = "apps.cpgtoacp.io/sandbox-cleanup"
	readyCondition   = "Ready"

	defaultWorkspace = "default"
	pollInterval     = 10 * time.Second
)

// OpenShellRunner runs an OpenShell CLI invocation. The interface keeps gateway
// calls deterministic and independently testable from Kubernetes reconciliation.
type OpenShellRunner interface {
	Run(ctx context.Context, args ...string) ([]byte, error)
}

// ExecOpenShellRunner invokes a local OpenShell CLI binary without a shell.
type ExecOpenShellRunner struct {
	Path string
}

// OpenShellCommandError retains CLI output so errors such as "not found" can be
// handled idempotently without matching the executable's exit status.
type OpenShellCommandError struct {
	Args   []string
	Output string
	Err    error
}

func (e *OpenShellCommandError) Error() string {
	operation := "command"
	if len(e.Args) >= 2 {
		operation = strings.Join(e.Args[:2], " ")
	}
	return fmt.Sprintf("openshell %s failed: %v: %s", operation, e.Err, strings.TrimSpace(e.Output))
}

func (e *OpenShellCommandError) Unwrap() error { return e.Err }

func (r ExecOpenShellRunner) Run(ctx context.Context, args ...string) ([]byte, error) {
	path := r.Path
	if path == "" {
		path = "openshell"
	}

	output, err := exec.CommandContext(ctx, path, args...).CombinedOutput() // #nosec G204 -- arguments are passed directly, never through a shell.
	if err != nil {
		return output, &OpenShellCommandError{Args: args, Output: string(output), Err: err}
	}
	return output, nil
}

// SandboxRequestReconciler reconciles a SandboxRequest object.
type SandboxRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Runner OpenShellRunner
}

// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=sandboxrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=sandboxrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=sandboxrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// Reconcile creates, observes, replaces, and deletes the OpenShell sandbox
// represented by a SandboxRequest.
func (r *SandboxRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var request appsv1alpha1.SandboxRequest
	if err := r.Get(ctx, req.NamespacedName, &request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !request.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &request)
	}

	if !containsString(request.Finalizers, sandboxFinalizer) {
		before := request.DeepCopy()
		request.Finalizers = append(request.Finalizers, sandboxFinalizer)
		if err := r.Patch(ctx, &request, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	policy, err := r.policy(ctx, &request)
	if err != nil {
		return r.fail(ctx, &request, "PolicyUnavailable", err)
	}

	desiredHash, err := requestHash(request.Spec, policy)
	if err != nil {
		return r.fail(ctx, &request, "InvalidSpec", err)
	}

	sandboxName := effectiveSandboxName(&request)
	if request.Status.SpecHash != "" && previousLocationChanged(&request, sandboxName) {
		previousRequest := request.DeepCopy()
		previousRequest.Spec.Gateway = request.Status.Gateway
		previousRequest.Spec.Workspace = request.Status.Workspace
		if err := r.deleteSandbox(ctx, previousRequest, request.Status.SandboxName); err != nil {
			return r.fail(ctx, &request, "DeleteFailed", err)
		}
		request.Status.SpecHash = ""
		request.Status.SandboxID = ""
	}

	observed, err := r.getSandbox(ctx, &request, sandboxName)
	if err == nil {
		if request.Status.SpecHash == "" {
			if observed.Labels["cpgtoacp.io/request-uid"] != string(request.UID) {
				collision := fmt.Errorf("sandbox %q already exists and is not owned by this request", sandboxName)
				return r.fail(ctx, &request, "NameCollision", collision)
			}
			request.Status.SpecHash = observed.Labels["cpgtoacp.io/spec-hash"]
		}
		if request.Status.SpecHash != desiredHash {
			if err := r.deleteSandbox(ctx, &request, sandboxName); err != nil {
				return r.fail(ctx, &request, "ReplaceFailed", err)
			}
			observed, err = r.createSandbox(ctx, &request, sandboxName, desiredHash, policy)
			if err != nil {
				return r.fail(ctx, &request, "CreateFailed", err)
			}
		}
	} else if isNotFound(err) {
		observed, err = r.createSandbox(ctx, &request, sandboxName, desiredHash, policy)
		if err != nil {
			return r.fail(ctx, &request, "CreateFailed", err)
		}
	} else {
		return r.fail(ctx, &request, "LookupFailed", err)
	}

	if err := r.recordObserved(ctx, &request, sandboxName, desiredHash, observed); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("reconciled OpenShell sandbox", "sandbox", sandboxName, "phase", observed.Phase)
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

func (r *SandboxRequestReconciler) reconcileDelete(ctx context.Context, request *appsv1alpha1.SandboxRequest) (ctrl.Result, error) {
	if !containsString(request.Finalizers, sandboxFinalizer) {
		return ctrl.Result{}, nil
	}

	location := request.DeepCopy()
	sandboxName := request.Status.SandboxName
	if sandboxName == "" {
		sandboxName = effectiveSandboxName(request)
	} else {
		location.Spec.Gateway = request.Status.Gateway
		location.Spec.Workspace = request.Status.Workspace
	}

	owned := request.Status.SpecHash != ""
	if !owned {
		observation, err := r.getSandbox(ctx, location, sandboxName)
		if err != nil && !isNotFound(err) {
			return r.fail(ctx, request, "DeleteLookupFailed", err)
		}
		owned = err == nil && observation.Labels["cpgtoacp.io/request-uid"] == string(request.UID)
	}
	if owned {
		if err := r.deleteSandbox(ctx, location, sandboxName); err != nil {
			return r.fail(ctx, request, "DeleteFailed", err)
		}
	}

	before := request.DeepCopy()
	request.Finalizers = removeString(request.Finalizers, sandboxFinalizer)
	return ctrl.Result{}, r.Patch(ctx, request, client.MergeFrom(before))
}

func (r *SandboxRequestReconciler) runner() OpenShellRunner {
	if r.Runner != nil {
		return r.Runner
	}
	return ExecOpenShellRunner{}
}

func (r *SandboxRequestReconciler) policy(ctx context.Context, request *appsv1alpha1.SandboxRequest) ([]byte, error) {
	selector := request.Spec.PolicyRef
	if selector == nil {
		return nil, nil
	}

	var configMap corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Name: selector.Name, Namespace: request.Namespace}, &configMap)
	if err != nil {
		if apierrors.IsNotFound(err) && selector.Optional != nil && *selector.Optional {
			return nil, nil
		}
		return nil, fmt.Errorf("read policy ConfigMap %s: %w", selector.Name, err)
	}
	if value, ok := configMap.Data[selector.Key]; ok {
		return []byte(value), nil
	}
	if value, ok := configMap.BinaryData[selector.Key]; ok {
		return value, nil
	}
	if selector.Optional != nil && *selector.Optional {
		return nil, nil
	}
	return nil, fmt.Errorf("policy key %q is missing from ConfigMap %s", selector.Key, selector.Name)
}

func requestHash(spec appsv1alpha1.SandboxRequestSpec, policy []byte) (string, error) {
	payload, err := json.Marshal(struct {
		Spec   appsv1alpha1.SandboxRequestSpec `json:"spec"`
		Policy []byte                          `json:"policy,omitempty"`
	}{Spec: spec, Policy: policy})
	if err != nil {
		return "", fmt.Errorf("encode desired sandbox state: %w", err)
	}
	sum := sha256.Sum256(payload)
	// OpenShell stores this value as a Kubernetes label, whose value is limited
	// to 63 characters. Dropping one hex nibble retains ample collision
	// resistance while keeping the hash valid as both a label and status value.
	return hex.EncodeToString(sum[:])[:63], nil
}

func effectiveSandboxName(request *appsv1alpha1.SandboxRequest) string {
	if request.Spec.SandboxName != "" {
		return request.Spec.SandboxName
	}
	return request.Name
}

func previousLocationChanged(request *appsv1alpha1.SandboxRequest, sandboxName string) bool {
	return request.Status.SandboxName != sandboxName ||
		request.Status.Gateway != request.Spec.Gateway ||
		request.Status.Workspace != workspace(request)
}

func workspace(request *appsv1alpha1.SandboxRequest) string {
	if request.Spec.Workspace != "" {
		return request.Spec.Workspace
	}
	return defaultWorkspace
}

func gatewayArgs(request *appsv1alpha1.SandboxRequest) []string {
	args := []string{"--workspace", workspace(request)}
	if request.Spec.Gateway.Name != "" {
		args = append(args, "--gateway", request.Spec.Gateway.Name)
	} else if request.Spec.Gateway.Endpoint != "" {
		args = append(args, "--gateway-endpoint", request.Spec.Gateway.Endpoint)
	}
	if request.Spec.Gateway.Insecure {
		args = append(args, "--gateway-insecure")
	}
	return args
}

func (r *SandboxRequestReconciler) getSandbox(ctx context.Context, request *appsv1alpha1.SandboxRequest, name string) (sandboxObservation, error) {
	args := []string{"sandbox", "get", name, "--output", "json"}
	args = append(args, gatewayArgs(request)...)
	output, err := r.runner().Run(ctx, args...)
	if err != nil {
		return sandboxObservation{}, err
	}
	return decodeSandbox(output)
}

func (r *SandboxRequestReconciler) createSandbox(
	ctx context.Context,
	request *appsv1alpha1.SandboxRequest,
	name string,
	specHash string,
	policy []byte,
) (sandboxObservation, error) {
	args := []string{"sandbox", "create", "--name", name, "--from", request.Spec.Image, "--detach", "--no-tty", "--no-auto-providers"}
	// OpenShell 0.0.111 does not allow --output together with a custom command.
	// Without a command, request JSON so the initial observation can be recorded.
	if len(request.Spec.Command) == 0 {
		args = append(args, "--output", "json")
	}
	args = append(args, gatewayArgs(request)...)

	if len(policy) > 0 {
		file, err := os.CreateTemp("", "cpgtoacp-openshell-policy-*.yaml")
		if err != nil {
			return sandboxObservation{}, fmt.Errorf("create temporary OpenShell policy: %w", err)
		}
		policyPath := file.Name()
		defer func() { _ = os.Remove(policyPath) }()
		if _, err := file.Write(policy); err != nil {
			_ = file.Close()
			return sandboxObservation{}, fmt.Errorf("write temporary OpenShell policy: %w", err)
		}
		if err := file.Close(); err != nil {
			return sandboxObservation{}, fmt.Errorf("close temporary OpenShell policy: %w", err)
		}
		args = append(args, "--policy", policyPath)
	}

	for _, provider := range request.Spec.Providers {
		args = append(args, "--provider", provider)
	}
	for _, item := range sortedPairs(request.Spec.Env) {
		args = append(args, "--env", item)
	}

	labels := make(map[string]string, len(request.Spec.Labels)+4)
	for key, value := range request.Spec.Labels {
		labels[key] = value
	}
	labels["cpgtoacp.io/request-name"] = request.Name
	labels["cpgtoacp.io/request-namespace"] = request.Namespace
	labels["cpgtoacp.io/request-uid"] = string(request.UID)
	labels["cpgtoacp.io/spec-hash"] = specHash
	for _, item := range sortedPairs(labels) {
		args = append(args, "--label", item)
	}

	if request.Spec.Resources.CPU != "" {
		args = append(args, "--cpu", request.Spec.Resources.CPU)
	}
	if request.Spec.Resources.Memory != "" {
		args = append(args, "--memory", request.Spec.Resources.Memory)
	}
	if request.Spec.Resources.GPU != nil {
		args = append(args, "--gpu", strconv.FormatInt(int64(*request.Spec.Resources.GPU), 10))
	}
	if request.Spec.ApprovalMode != "" {
		args = append(args, "--approval-mode", request.Spec.ApprovalMode)
	}
	if len(request.Spec.Command) > 0 {
		args = append(args, "--")
		args = append(args, request.Spec.Command...)
	}

	output, err := r.runner().Run(ctx, args...)
	if err != nil {
		return sandboxObservation{}, err
	}
	observation, err := decodeSandbox(output)
	if err != nil && len(request.Spec.Command) > 0 {
		// Command-bearing creates produce human-readable output. The normal poll
		// will retrieve structured state after the gateway accepts the request.
		return sandboxObservation{Phase: "Creating"}, nil
	}
	if err != nil {
		return sandboxObservation{}, err
	}
	if observation.Phase == "" {
		observation.Phase = "Creating"
	}
	return observation, nil
}

func (r *SandboxRequestReconciler) deleteSandbox(ctx context.Context, request *appsv1alpha1.SandboxRequest, name string) error {
	args := []string{"sandbox", "delete", name}
	args = append(args, gatewayArgs(request)...)
	_, err := r.runner().Run(ctx, args...)
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

type sandboxObservation struct {
	ID     string
	Phase  string
	Labels map[string]string
}

func decodeSandbox(output []byte) (sandboxObservation, error) {
	if len(strings.TrimSpace(string(output))) == 0 {
		return sandboxObservation{Phase: "Creating"}, nil
	}

	var value any
	if err := json.Unmarshal(output, &value); err != nil {
		return sandboxObservation{}, fmt.Errorf("decode OpenShell JSON response: %w", err)
	}
	return sandboxObservation{
		ID:     findString(value, "id", "sandbox_id", "sandboxId"),
		Phase:  findString(value, "phase", "state", "status"),
		Labels: findStringMap(value, "labels"),
	}, nil
}

func findString(value any, keys ...string) string {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range keys {
			if candidate, ok := typed[key].(string); ok {
				return candidate
			}
		}
		for _, child := range typed {
			if candidate := findString(child, keys...); candidate != "" {
				return candidate
			}
		}
	case []any:
		for _, child := range typed {
			if candidate := findString(child, keys...); candidate != "" {
				return candidate
			}
		}
	}
	return ""
}

func findStringMap(value any, key string) map[string]string {
	switch typed := value.(type) {
	case map[string]any:
		if raw, ok := typed[key].(map[string]any); ok {
			result := make(map[string]string, len(raw))
			for label, value := range raw {
				if stringValue, ok := value.(string); ok {
					result[label] = stringValue
				}
			}
			return result
		}
		for _, child := range typed {
			if result := findStringMap(child, key); result != nil {
				return result
			}
		}
	case []any:
		for _, child := range typed {
			if result := findStringMap(child, key); result != nil {
				return result
			}
		}
	}
	return nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var commandErr *OpenShellCommandError
	message := err.Error()
	if errors.As(err, &commandErr) {
		message = commandErr.Output
	}
	message = strings.ToLower(message)
	return strings.Contains(message, "not found") ||
		strings.Contains(message, "does not exist") ||
		strings.Contains(message, "no sandbox")
}

func (r *SandboxRequestReconciler) recordObserved(
	ctx context.Context,
	request *appsv1alpha1.SandboxRequest,
	name string,
	specHash string,
	observation sandboxObservation,
) error {
	before := request.DeepCopy()
	request.Status.SandboxName = name
	request.Status.SandboxID = observation.ID
	request.Status.Gateway = request.Spec.Gateway
	request.Status.Workspace = workspace(request)
	request.Status.SpecHash = specHash
	request.Status.ObservedGeneration = request.Generation
	request.Status.Phase = observation.Phase

	condition := metav1.Condition{
		Type:               readyCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: request.Generation,
		Reason:             "SandboxProgressing",
		Message:            fmt.Sprintf("OpenShell sandbox %q is %s", name, displayPhase(observation.Phase)),
	}
	switch strings.ToLower(observation.Phase) {
	case "ready", "running":
		condition.Status = metav1.ConditionTrue
		condition.Reason = "SandboxReady"
	case "failed", "error":
		condition.Reason = "SandboxFailed"
	}
	meta.SetStatusCondition(&request.Status.Conditions, condition)
	return r.Status().Patch(ctx, request, client.MergeFrom(before))
}

func (r *SandboxRequestReconciler) fail(
	ctx context.Context,
	request *appsv1alpha1.SandboxRequest,
	reason string,
	cause error,
) (ctrl.Result, error) {
	before := request.DeepCopy()
	request.Status.ObservedGeneration = request.Generation
	request.Status.Phase = "Error"
	meta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{
		Type:               readyCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: request.Generation,
		Reason:             reason,
		Message:            cause.Error(),
	})
	if err := r.Status().Patch(ctx, request, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, errors.Join(cause, err)
	}
	return ctrl.Result{}, cause
}

func displayPhase(phase string) string {
	if phase == "" {
		return "being created"
	}
	return phase
}

func sortedPairs(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	sort.Strings(result)
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func removeString(values []string, target string) []string {
	result := values[:0]
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}

func (r *SandboxRequestReconciler) requestsForPolicy(ctx context.Context, object client.Object) []reconcile.Request {
	var requests appsv1alpha1.SandboxRequestList
	if err := r.List(ctx, &requests, client.InNamespace(object.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "unable to list SandboxRequests for changed policy ConfigMap")
		return nil
	}

	result := make([]reconcile.Request, 0)
	for i := range requests.Items {
		request := &requests.Items[i]
		if request.Spec.PolicyRef != nil && request.Spec.PolicyRef.Name == object.GetName() {
			result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)})
		}
	}
	return result
}

// SetupWithManager sets up the controller with the Manager.
func (r *SandboxRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.SandboxRequest{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPolicy)).
		Named("sandboxrequest").
		Complete(r)
}
