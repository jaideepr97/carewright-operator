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
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	openshellv1 "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	openshelltypes "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1/types"
	sandboxv1 "github.com/NVIDIA/OpenShell/sdk/go/proto/sandboxv1"
	"google.golang.org/protobuf/encoding/protojson"
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
	"sigs.k8s.io/yaml"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"
)

const (
	sandboxFinalizer = "apps.carewright.io/sandbox-cleanup"
	readyCondition   = "Ready"

	defaultWorkspace      = "default"
	defaultMainShell      = "/bin/bash"
	proposalApprovalMode  = "proposal_approval_mode"
	providerSourceKey     = "carewright.io/credential-secret"
	providerSecretUID     = "carewright.io/credential-secret-uid"
	providerSecretVersion = "carewright.io/credential-secret-version"
	pollInterval          = 10 * time.Second
)

// OpenShellClientSession owns an SDK client for one gateway connection.
type OpenShellClientSession struct {
	Client openshellv1.ClientInterface
	Close  func() error
}

// OpenShellClientFactory creates SDK clients for the gateway selected by a request.
type OpenShellClientFactory interface {
	NewClient(gateway appsv1alpha1.SandboxGatewaySpec) (*OpenShellClientSession, error)
}

// SDKOpenShellClientFactory creates official OpenShell Go SDK clients.
type SDKOpenShellClientFactory struct{}

func (SDKOpenShellClientFactory) NewClient(gateway appsv1alpha1.SandboxGatewaySpec) (*OpenShellClientSession, error) {
	if gateway.Name != "" {
		return nil, fmt.Errorf("gateway.name %q requires CLI registration and is not supported by the SDK; set gateway.endpoint", gateway.Name)
	}

	endpoint := gateway.Endpoint
	insecure := gateway.Insecure
	if endpoint == "" {
		endpoint = os.Getenv("OPENSHELL_GATEWAY_ENDPOINT")
		if !insecure {
			insecure, _ = strconv.ParseBool(os.Getenv("OPENSHELL_GATEWAY_INSECURE"))
		}
	}
	if endpoint == "" {
		return nil, errors.New("OpenShell gateway endpoint is required in spec.gateway.endpoint or OPENSHELL_GATEWAY_ENDPOINT")
	}

	config := openshellv1.Config{Address: endpoint, Auth: openshellv1.NoAuth()}
	if insecure && !strings.HasPrefix(endpoint, "http://") {
		config.TLS = &openshelltypes.TLSConfig{Insecure: true}
	}
	sdkClient, err := openshellv1.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("create OpenShell SDK client: %w", err)
	}
	return &OpenShellClientSession{Client: sdkClient, Close: sdkClient.Close}, nil
}

// SandboxRequestReconciler reconciles a SandboxRequest object.
type SandboxRequestReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	ClientFactory OpenShellClientFactory
}

// +kubebuilder:rbac:groups=apps.carewright.io,resources=sandboxrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.carewright.io,resources=sandboxrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.carewright.io,resources=sandboxrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch

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
	if err := r.registerProviders(ctx, &request); err != nil {
		return r.fail(ctx, &request, "ProviderUnavailable", err)
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
			if observed.Labels["carewright.io/request-uid"] != string(request.UID) {
				collision := fmt.Errorf("sandbox %q already exists and is not owned by this request", sandboxName)
				return r.fail(ctx, &request, "NameCollision", collision)
			}
			request.Status.SpecHash = observed.Labels["carewright.io/spec-hash"]
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
	} else if openshellv1.IsNotFound(err) {
		observed, err = r.createSandbox(ctx, &request, sandboxName, desiredHash, policy)
		if err != nil {
			return r.fail(ctx, &request, "CreateFailed", err)
		}
	} else {
		return r.fail(ctx, &request, "LookupFailed", err)
	}

	var services []appsv1alpha1.SandboxServiceStatus
	if sandboxReady(observed.Phase) {
		services, err = r.reconcileServices(ctx, &request, sandboxName)
		if err != nil {
			return r.fail(ctx, &request, "ServiceExposureFailed", err)
		}
	}

	cleanupPending, err := r.cleanupStaleProviders(ctx, &request)
	if err != nil {
		return r.fail(ctx, &request, "ProviderCleanupFailed", err)
	}
	if err := r.recordObserved(ctx, &request, sandboxName, desiredHash, observed, services); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("reconciled OpenShell sandbox", "sandbox", sandboxName, "phase", observed.Phase)
	if cleanupPending {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
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
		if err != nil && !openshellv1.IsNotFound(err) {
			return r.fail(ctx, request, "DeleteLookupFailed", err)
		}
		owned = err == nil && observation.Labels["carewright.io/request-uid"] == string(request.UID)
	}
	if owned {
		if err := r.deleteSandbox(ctx, location, sandboxName); err != nil {
			return r.fail(ctx, request, "DeleteFailed", err)
		}
	}

	registrations := slices.Clone(request.Status.ManagedProviders)
	for _, desired := range desiredManagedProviders(request) {
		if !slices.Contains(registrations, desired) {
			registrations = append(registrations, desired)
		}
	}
	for _, registration := range registrations {
		released, err := r.releaseProvider(ctx, request, registration, true)
		if err != nil {
			return r.fail(ctx, request, "ProviderCleanupFailed", err)
		}
		if !released {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	before := request.DeepCopy()
	request.Finalizers = removeString(request.Finalizers, sandboxFinalizer)
	return ctrl.Result{}, r.Patch(ctx, request, client.MergeFrom(before))
}

func (r *SandboxRequestReconciler) clientSession(gateway appsv1alpha1.SandboxGatewaySpec) (*OpenShellClientSession, error) {
	factory := r.ClientFactory
	if factory == nil {
		factory = SDKOpenShellClientFactory{}
	}
	return factory.NewClient(gateway)
}

func closeSession(session *OpenShellClientSession) {
	if session != nil && session.Close != nil {
		_ = session.Close()
	}
}

// registerProviders ensures Secret-backed providers exist before the sandbox is
// created. The provider name is shared by components in one pipeline, so a
// Secret version marker prevents unnecessary updates on every poll.
func (r *SandboxRequestReconciler) registerProviders(ctx context.Context, request *appsv1alpha1.SandboxRequest) error {
	if len(request.Spec.ProviderRegistrations) == 0 {
		return nil
	}
	session, err := r.clientSession(request.Spec.Gateway)
	if err != nil {
		return err
	}
	defer closeSession(session)

	for _, registration := range request.Spec.ProviderRegistrations {
		if registration.Name == "" || registration.Type == "" || registration.SecretRef.Name == "" {
			return fmt.Errorf("provider registration requires name, type, and secretRef.name")
		}
		if !slices.Contains(request.Spec.Providers, registration.Name) {
			return fmt.Errorf("provider registration %q is not listed in spec.providers", registration.Name)
		}
		var secret corev1.Secret
		key := types.NamespacedName{Name: registration.SecretRef.Name, Namespace: request.Namespace}
		if err := r.Get(ctx, key, &secret); err != nil {
			return fmt.Errorf("read provider Secret %s: %w", key, err)
		}
		if len(secret.Data) == 0 {
			return fmt.Errorf("provider Secret %s contains no credentials", key)
		}
		credentials := make(map[string]string, len(secret.Data))
		for name, value := range secret.Data {
			credentials[name] = string(value)
		}
		source := key.String()
		existing, err := session.Client.Providers().Get(ctx, workspace(request), registration.Name)
		if err != nil && !openshellv1.IsNotFound(err) {
			return fmt.Errorf("get OpenShell provider %q: %w", registration.Name, err)
		}
		if err == nil {
			if existing.Annotations[providerSourceKey] != source {
				return fmt.Errorf("OpenShell provider %q already exists and is not managed from Secret %s", registration.Name, source)
			}
			if existing.Annotations[providerSecretUID] == string(secret.UID) &&
				existing.Annotations[providerSecretVersion] == secret.ResourceVersion &&
				existing.Type == registration.Type &&
				maps.Equal(existing.Spec.Config, registration.Config) {
				if err := r.trackManagedProvider(ctx, request, managedProviderStatus(request, registration)); err != nil {
					return err
				}
				continue
			}
		}
		provider := &openshellv1.Provider{
			Name: registration.Name,
			Type: registration.Type,
			Annotations: map[string]string{
				providerSourceKey:     source,
				providerSecretUID:     string(secret.UID),
				providerSecretVersion: secret.ResourceVersion,
			},
			Spec: openshellv1.ProviderSpec{
				Credentials: credentials,
				Config:      maps.Clone(registration.Config),
			},
		}
		if existing == nil {
			_, err = session.Client.Providers().Create(ctx, workspace(request), provider)
		} else {
			provider.ID = existing.ID
			provider.ResourceVersion = existing.ResourceVersion
			provider.Labels = maps.Clone(existing.Labels)
			for name, value := range existing.Annotations {
				if _, managed := provider.Annotations[name]; !managed {
					provider.Annotations[name] = value
				}
			}
			_, err = session.Client.Providers().Update(ctx, workspace(request), provider)
		}
		if err != nil {
			return fmt.Errorf("register OpenShell provider %q: %w", registration.Name, err)
		}
		if err := r.trackManagedProvider(ctx, request, managedProviderStatus(request, registration)); err != nil {
			return err
		}
	}
	return nil
}

func managedProviderStatus(request *appsv1alpha1.SandboxRequest, registration appsv1alpha1.SandboxProviderRegistration) appsv1alpha1.ManagedProviderStatus {
	return appsv1alpha1.ManagedProviderStatus{
		Name:       registration.Name,
		Gateway:    resolvedGatewaySpec(request.Spec.Gateway),
		Workspace:  workspace(request),
		SecretName: registration.SecretRef.Name,
	}
}

func resolvedGatewaySpec(gateway appsv1alpha1.SandboxGatewaySpec) appsv1alpha1.SandboxGatewaySpec {
	if gateway.Endpoint == "" {
		gateway.Endpoint = os.Getenv("OPENSHELL_GATEWAY_ENDPOINT")
		if !gateway.Insecure {
			gateway.Insecure, _ = strconv.ParseBool(os.Getenv("OPENSHELL_GATEWAY_INSECURE"))
		}
	}
	return gateway
}

func desiredManagedProviders(request *appsv1alpha1.SandboxRequest) []appsv1alpha1.ManagedProviderStatus {
	result := make([]appsv1alpha1.ManagedProviderStatus, 0, len(request.Spec.ProviderRegistrations))
	for _, registration := range request.Spec.ProviderRegistrations {
		result = append(result, managedProviderStatus(request, registration))
	}
	return result
}

func providerGatewayAddress(gateway appsv1alpha1.SandboxGatewaySpec) string {
	if gateway.Name != "" {
		return "name:" + gateway.Name
	}
	endpoint := gateway.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("OPENSHELL_GATEWAY_ENDPOINT")
	}
	return strings.TrimRight(endpoint, "/")
}

func sameProviderLocation(left, right appsv1alpha1.ManagedProviderStatus) bool {
	return left.Name == right.Name &&
		left.Workspace == right.Workspace &&
		providerGatewayAddress(left.Gateway) == providerGatewayAddress(right.Gateway)
}

func (r *SandboxRequestReconciler) trackManagedProvider(ctx context.Context, request *appsv1alpha1.SandboxRequest, registration appsv1alpha1.ManagedProviderStatus) error {
	if slices.Contains(request.Status.ManagedProviders, registration) {
		return nil
	}
	before := request.DeepCopy()
	request.Status.ManagedProviders = append(request.Status.ManagedProviders, registration)
	return r.Status().Patch(ctx, request, client.MergeFrom(before))
}

func (r *SandboxRequestReconciler) cleanupStaleProviders(ctx context.Context, request *appsv1alpha1.SandboxRequest) (bool, error) {
	desired := desiredManagedProviders(request)
	remaining := make([]appsv1alpha1.ManagedProviderStatus, 0, len(request.Status.ManagedProviders))
	pending := false
	for _, registration := range request.Status.ManagedProviders {
		if slices.Contains(desired, registration) {
			remaining = append(remaining, registration)
			continue
		}
		// Keep tracking a provider still attached to this sandbox, even if
		// its registration was removed from the desired spec.
		if providerGatewayAddress(request.Spec.Gateway) == providerGatewayAddress(registration.Gateway) &&
			workspace(request) == registration.Workspace &&
			slices.Contains(request.Spec.Providers, registration.Name) {
			remaining = append(remaining, registration)
			continue
		}
		released, err := r.releaseProvider(ctx, request, registration, false)
		if err != nil {
			return false, err
		}
		if !released {
			remaining = append(remaining, registration)
			pending = true
		}
	}
	if !slices.Equal(request.Status.ManagedProviders, remaining) {
		before := request.DeepCopy()
		request.Status.ManagedProviders = remaining
		if err := r.Status().Patch(ctx, request, client.MergeFrom(before)); err != nil {
			return false, err
		}
	}
	return pending, nil
}

// releaseProvider returns false while a deleting peer still needs the provider.
// This leaves one finalizer in place so concurrent deletions cannot orphan it.
func (r *SandboxRequestReconciler) releaseProvider(ctx context.Context, request *appsv1alpha1.SandboxRequest, registration appsv1alpha1.ManagedProviderStatus, deleting bool) (bool, error) {
	var requests appsv1alpha1.SandboxRequestList
	if err := r.List(ctx, &requests); err != nil {
		return false, fmt.Errorf("list SandboxRequests using provider %q: %w", registration.Name, err)
	}
	currentKey := request.Namespace + "/" + request.Name
	for i := range requests.Items {
		other := &requests.Items[i]
		otherKey := other.Namespace + "/" + other.Name
		if otherKey == currentKey || !requestReferencesProvider(other, registration) {
			continue
		}
		// A consumer or manager without a finalizer cannot clean it up for us.
		if other.Namespace != request.Namespace || !requestManagesProvider(other, registration) || !containsString(other.Finalizers, sandboxFinalizer) {
			return false, nil
		}
		if other.DeletionTimestamp.IsZero() {
			return true, nil
		}
		if !deleting || otherKey < currentKey {
			return false, nil
		}
		// A deleting request with a smaller key removes its finalizer first.
		return true, nil
	}

	session, err := r.clientSession(registration.Gateway)
	if err != nil {
		return false, err
	}
	defer closeSession(session)
	provider, err := session.Client.Providers().Get(ctx, registration.Workspace, registration.Name)
	if openshellv1.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get OpenShell provider %q for cleanup: %w", registration.Name, err)
	}
	source := request.Namespace + "/" + registration.SecretName
	if provider.Annotations[providerSourceKey] != source {
		return true, nil
	}
	result, err := session.Client.Providers().Delete(ctx, registration.Workspace, registration.Name)
	if openshellv1.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("delete OpenShell provider %q: %w", registration.Name, err)
	}
	return deletionComplete(result), nil
}

func requestManagesProvider(request *appsv1alpha1.SandboxRequest, registration appsv1alpha1.ManagedProviderStatus) bool {
	for _, tracked := range request.Status.ManagedProviders {
		if sameProviderLocation(tracked, registration) && tracked.SecretName == registration.SecretName {
			return true
		}
	}
	current := appsv1alpha1.ManagedProviderStatus{
		Name:      registration.Name,
		Gateway:   request.Spec.Gateway,
		Workspace: workspace(request),
	}
	if !sameProviderLocation(current, registration) {
		return false
	}
	for _, desired := range request.Spec.ProviderRegistrations {
		if desired.Name == registration.Name && desired.SecretRef.Name == registration.SecretName {
			return true
		}
	}
	return false
}

func requestReferencesProvider(request *appsv1alpha1.SandboxRequest, registration appsv1alpha1.ManagedProviderStatus) bool {
	current := appsv1alpha1.ManagedProviderStatus{
		Name:      registration.Name,
		Gateway:   request.Spec.Gateway,
		Workspace: workspace(request),
	}
	if sameProviderLocation(current, registration) && slices.Contains(request.Spec.Providers, registration.Name) {
		return true
	}
	for _, tracked := range request.Status.ManagedProviders {
		if sameProviderLocation(tracked, registration) {
			return true
		}
	}
	return false
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
	// Gateway service exposure is reconciled independently and must not replace
	// an otherwise healthy sandbox when a port or service name changes.
	spec.Services = nil
	payload, err := json.Marshal(struct {
		Spec   appsv1alpha1.SandboxRequestSpec `json:"spec"`
		Policy []byte                          `json:"policy,omitempty"`
	}{Spec: spec, Policy: policy})
	if err != nil {
		return "", fmt.Errorf("encode desired sandbox state: %w", err)
	}
	sum := sha256.Sum256(payload)
	// OpenShell labels share Kubernetes' 63-character label-value limit.
	return hex.EncodeToString(sum[:])[:63], nil
}

func (r *SandboxRequestReconciler) reconcileServices(
	ctx context.Context,
	request *appsv1alpha1.SandboxRequest,
	sandboxName string,
) ([]appsv1alpha1.SandboxServiceStatus, error) {
	session, err := r.clientSession(request.Spec.Gateway)
	if err != nil {
		return nil, err
	}
	defer closeSession(session)

	desired := make(map[string]appsv1alpha1.SandboxServiceSpec, len(request.Spec.Services))
	for _, service := range request.Spec.Services {
		desired[service.Name] = service
	}
	for _, previous := range request.Status.Services {
		if _, keep := desired[previous.Name]; keep {
			continue
		}
		result, err := session.Client.Services().Delete(ctx, workspace(request), sandboxName, previous.Name)
		if err != nil && !openshellv1.IsNotFound(err) {
			return nil, fmt.Errorf("delete stale service %q: %w", previous.Name, err)
		}
		if err == nil && !deletionComplete(result) {
			return nil, fmt.Errorf("deletion of stale service %q is pending", previous.Name)
		}
	}

	result := make([]appsv1alpha1.SandboxServiceStatus, 0, len(request.Spec.Services))
	for _, service := range request.Spec.Services {
		endpoint, err := session.Client.Services().Get(ctx, workspace(request), sandboxName, service.Name)
		needsExposure := openshellv1.IsNotFound(err)
		if err == nil && (endpoint.TargetPort != uint32(service.TargetPort) || !endpoint.Domain || endpoint.URL == "") { // #nosec G115 -- CRD validation limits the port to uint16 range.
			result, err := session.Client.Services().Delete(ctx, workspace(request), sandboxName, service.Name)
			if err != nil && !openshellv1.IsNotFound(err) {
				return nil, fmt.Errorf("replace service %q: %w", service.Name, err)
			}
			if err == nil && !deletionComplete(result) {
				return nil, fmt.Errorf("deletion of service %q is pending", service.Name)
			}
			needsExposure = true
		}
		if needsExposure {
			endpoint, err = session.Client.Services().Expose(ctx, workspace(request), sandboxName, service.Name, uint32(service.TargetPort), true) // #nosec G115 -- CRD validation limits the port to uint16 range.
		}
		if err != nil {
			return nil, fmt.Errorf("expose service %q on port %d: %w", service.Name, service.TargetPort, err)
		}
		if endpoint == nil || endpoint.URL == "" {
			return nil, fmt.Errorf("expose service %q on port %d: gateway returned no URL", service.Name, service.TargetPort)
		}
		result = append(result, appsv1alpha1.SandboxServiceStatus{
			Name:       service.Name,
			TargetPort: int32(endpoint.TargetPort), // #nosec G115 -- target ports are constrained to uint16 range.
			ID:         endpoint.ID,
			URL:        endpoint.URL,
		})
	}
	return result, nil
}

func effectiveSandboxName(request *appsv1alpha1.SandboxRequest) string {
	if request.Spec.SandboxName != "" {
		return request.Spec.SandboxName
	}
	return request.Name
}

func previousLocationChanged(request *appsv1alpha1.SandboxRequest, sandboxName string) bool {
	return request.Status.SandboxName != sandboxName ||
		request.Status.Gateway != resolvedGatewaySpec(request.Spec.Gateway) ||
		request.Status.Workspace != workspace(request)
}

func workspace(request *appsv1alpha1.SandboxRequest) string {
	if request.Spec.Workspace != "" {
		return request.Spec.Workspace
	}
	return defaultWorkspace
}

func (r *SandboxRequestReconciler) getSandbox(ctx context.Context, request *appsv1alpha1.SandboxRequest, name string) (sandboxObservation, error) {
	session, err := r.clientSession(request.Spec.Gateway)
	if err != nil {
		return sandboxObservation{}, err
	}
	defer closeSession(session)

	sandbox, err := session.Client.Sandboxes().Get(ctx, workspace(request), name)
	if err != nil {
		return sandboxObservation{}, err
	}
	return observeSandbox(sandbox), nil
}

func (r *SandboxRequestReconciler) createSandbox(
	ctx context.Context,
	request *appsv1alpha1.SandboxRequest,
	name string,
	specHash string,
	policyData []byte,
) (sandboxObservation, error) {
	policy, err := decodeSandboxPolicy(policyData)
	if err != nil {
		return sandboxObservation{}, err
	}
	policy = mergeNetworkAccess(policy, request.Spec.NetworkAccess)

	labels := make(map[string]string, len(request.Spec.Labels)+4)
	for key, value := range request.Spec.Labels {
		labels[key] = value
	}
	labels["carewright.io/request-name"] = request.Name
	labels["carewright.io/request-namespace"] = request.Namespace
	labels["carewright.io/request-uid"] = string(request.UID)
	labels["carewright.io/spec-hash"] = specHash

	command := slices.Clone(request.Spec.Command)
	if len(command) == 0 {
		command = []string{defaultMainShell, "-l"}
	}
	template := &openshellv1.SandboxTemplate{
		Image:     request.Spec.Image,
		Resources: resourceLimits(request.Spec.Resources),
	}
	spec := &openshellv1.SandboxSpec{
		Environment: cloneMap(request.Spec.Env),
		Template:    template,
		Providers:   slices.Clone(request.Spec.Providers),
		Policy:      policy,
		Command:     command,
		TTY:         false,
	}
	if request.Spec.Resources.GPU != nil {
		count := uint32(*request.Spec.Resources.GPU) // #nosec G115 -- CRD validation requires a non-negative value.
		spec.GPUCount = &count
	}

	session, err := r.clientSession(request.Spec.Gateway)
	if err != nil {
		return sandboxObservation{}, err
	}
	defer closeSession(session)

	sandbox, err := session.Client.Sandboxes().Create(ctx, workspace(request), name, spec, labels)
	if err != nil {
		return sandboxObservation{}, err
	}
	if request.Spec.ApprovalMode == "auto" {
		_, err = session.Client.Config().Update(ctx, workspace(request), &openshellv1.ConfigUpdate{
			Name:       name,
			SettingKey: proposalApprovalMode,
			SettingValue: &openshellv1.SettingValue{
				Type:      openshellv1.SettingValueString,
				StringVal: request.Spec.ApprovalMode,
			},
		})
		if err != nil {
			result, deleteErr := session.Client.Sandboxes().Delete(ctx, workspace(request), name)
			if deleteErr == nil && !deletionComplete(result) {
				deleteErr = fmt.Errorf("sandbox %q deletion is pending", name)
			}
			return sandboxObservation{}, errors.Join(fmt.Errorf("set sandbox approval mode: %w", err), deleteErr)
		}
	}
	return observeSandbox(sandbox), nil
}

func (r *SandboxRequestReconciler) deleteSandbox(ctx context.Context, request *appsv1alpha1.SandboxRequest, name string) error {
	session, err := r.clientSession(request.Spec.Gateway)
	if err != nil {
		return err
	}
	defer closeSession(session)

	result, err := session.Client.Sandboxes().Delete(ctx, workspace(request), name)
	if openshellv1.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !deletionComplete(result) {
		return fmt.Errorf("sandbox %q deletion is pending", name)
	}
	return nil
}

func deletionComplete(result *openshellv1.DeletionResult) bool {
	return result != nil && (result.Outcome == openshellv1.DeletionCompleted || result.Outcome == openshellv1.DeletionAlreadyAbsent)
}

func resourceLimits(resources appsv1alpha1.SandboxResources) map[string]any {
	limits := map[string]any{}
	if resources.CPU != "" {
		limits["cpu"] = resources.CPU
	}
	if resources.Memory != "" {
		limits["memory"] = resources.Memory
	}
	if len(limits) == 0 {
		return nil
	}
	return map[string]any{"limits": limits}
}

func decodeSandboxPolicy(data []byte) (*openshellv1.SandboxPolicy, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("decode OpenShell policy YAML: %w", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(jsonData, &document); err != nil {
		return nil, fmt.Errorf("decode OpenShell policy document: %w", err)
	}
	for oldName, protoName := range map[string]string{
		"filesystem_policy": "filesystem",
		"landlock_policy":   "landlock",
		"process_policy":    "process",
	} {
		if value, ok := document[oldName]; ok {
			if _, duplicate := document[protoName]; duplicate {
				return nil, fmt.Errorf("OpenShell policy contains both %q and %q", oldName, protoName)
			}
			document[protoName] = value
			delete(document, oldName)
		}
	}
	if err := normalizePolicyEndpointEnums(document); err != nil {
		return nil, err
	}
	protoData, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode OpenShell policy document: %w", err)
	}
	var protoPolicy sandboxv1.SandboxPolicy
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(protoData, &protoPolicy); err != nil {
		return nil, fmt.Errorf("validate OpenShell policy: %w", err)
	}
	canonical, err := (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(&protoPolicy)
	if err != nil {
		return nil, fmt.Errorf("encode validated OpenShell policy: %w", err)
	}
	var policy openshellv1.SandboxPolicy
	if err := json.Unmarshal(canonical, &policy); err != nil {
		return nil, fmt.Errorf("convert OpenShell policy to SDK types: %w", err)
	}
	return &policy, nil
}

func normalizePolicyEndpointEnums(document map[string]json.RawMessage) error {
	raw, ok := document["network_policies"]
	if !ok {
		return nil
	}
	var rules map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rules); err != nil {
		return fmt.Errorf("decode OpenShell network policies: %w", err)
	}
	aliases := map[string]map[string]int{
		"tls":         {"auto": 0, "skip": 1, "terminate": 2, "passthrough": 3},
		"enforcement": {"enforce": 1, "audit": 2},
		"access":      {"read_only": 1, "read_write": 2, "full": 3},
	}
	for _, rule := range rules {
		rawEndpoints, ok := rule["endpoints"]
		if !ok {
			continue
		}
		var endpoints []map[string]json.RawMessage
		if err := json.Unmarshal(rawEndpoints, &endpoints); err != nil {
			return fmt.Errorf("decode OpenShell network endpoints: %w", err)
		}
		for _, endpoint := range endpoints {
			for field, values := range aliases {
				var value string
				if err := json.Unmarshal(endpoint[field], &value); err != nil {
					continue
				}
				if numeric, ok := values[value]; ok {
					endpoint[field], _ = json.Marshal(numeric)
				}
			}
		}
		rule["endpoints"], _ = json.Marshal(endpoints)
	}
	document["network_policies"], _ = json.Marshal(rules)
	return nil
}

func mergeNetworkAccess(policy *openshellv1.SandboxPolicy, access []appsv1alpha1.SandboxNetworkAccessSpec) *openshellv1.SandboxPolicy {
	if len(access) == 0 {
		return policy
	}
	if policy == nil {
		policy = &openshellv1.SandboxPolicy{Version: 1}
	}
	if policy.NetworkPolicies == nil {
		policy.NetworkPolicies = map[string]openshellv1.NetworkPolicyRule{}
	}
	for _, endpoint := range access {
		protocol := endpoint.Protocol
		if protocol == "" || protocol == "http" || protocol == "https" {
			protocol = "rest"
		}
		policy.NetworkPolicies[endpoint.Name] = openshellv1.NetworkPolicyRule{
			Name: endpoint.Name,
			Endpoints: []openshellv1.PolicyNetworkEndpoint{{
				Host:        endpoint.Host,
				Port:        uint32(endpoint.Port), // #nosec G115 -- CRD validation limits the port to uint16 range.
				Protocol:    protocol,
				Enforcement: openshelltypes.NetworkEnforcementModeEnforce,
				Access:      openshelltypes.NetworkAccessPresetFull,
			}},
			Binaries: []openshellv1.PolicyNetworkBinary{{Path: "**"}},
		}
	}
	return policy
}

type sandboxObservation struct {
	ID     string
	Phase  string
	Labels map[string]string
}

func observeSandbox(sandbox *openshellv1.Sandbox) sandboxObservation {
	if sandbox == nil {
		return sandboxObservation{Phase: string(openshelltypes.SandboxUnknown)}
	}
	return sandboxObservation{
		ID:     sandbox.ID,
		Phase:  string(sandbox.Status.Phase),
		Labels: cloneMap(sandbox.Labels),
	}
}

func (r *SandboxRequestReconciler) recordObserved(
	ctx context.Context,
	request *appsv1alpha1.SandboxRequest,
	name string,
	specHash string,
	observation sandboxObservation,
	services []appsv1alpha1.SandboxServiceStatus,
) error {
	before := request.DeepCopy()
	request.Status.SandboxName = name
	request.Status.SandboxID = observation.ID
	request.Status.Gateway = resolvedGatewaySpec(request.Spec.Gateway)
	request.Status.Workspace = workspace(request)
	request.Status.SpecHash = specHash
	request.Status.ObservedGeneration = request.Generation
	request.Status.Phase = observation.Phase
	request.Status.Services = services

	condition := metav1.Condition{
		Type:               readyCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: request.Generation,
		Reason:             "SandboxProgressing",
		Message:            fmt.Sprintf("OpenShell sandbox %q is %s", name, displayPhase(observation.Phase)),
	}
	switch {
	case sandboxReady(observation.Phase) && len(request.Status.Services) == len(request.Spec.Services):
		condition.Status = metav1.ConditionTrue
		condition.Reason = "SandboxReady"
		if len(request.Spec.Services) != 0 {
			condition.Reason = "SandboxServicesReady"
			condition.Message = fmt.Sprintf("OpenShell sandbox %q and all %d services are ready", name, len(request.Spec.Services))
		}
	case strings.EqualFold(observation.Phase, "failed"), strings.EqualFold(observation.Phase, "error"):
		condition.Reason = "SandboxFailed"
	}
	meta.SetStatusCondition(&request.Status.Conditions, condition)
	return r.Status().Patch(ctx, request, client.MergeFrom(before))
}

func sandboxReady(phase string) bool {
	return strings.EqualFold(phase, "ready") || strings.EqualFold(phase, "running")
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

func cloneMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
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

func (r *SandboxRequestReconciler) requestsForProviderSecret(ctx context.Context, object client.Object) []reconcile.Request {
	var requests appsv1alpha1.SandboxRequestList
	if err := r.List(ctx, &requests, client.InNamespace(object.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "unable to list SandboxRequests for changed provider Secret")
		return nil
	}
	result := make([]reconcile.Request, 0)
	for i := range requests.Items {
		request := &requests.Items[i]
		for _, registration := range request.Spec.ProviderRegistrations {
			if registration.SecretRef.Name == object.GetName() {
				result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)})
				break
			}
		}
	}
	return result
}

// SetupWithManager sets up the controller with the Manager.
func (r *SandboxRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.SandboxRequest{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPolicy)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProviderSecret)).
		Named("sandboxrequest").
		Complete(r)
}
