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
	"testing"
	"time"

	openshellv1 "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	openshellfake "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1/fake"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"
)

type fakeOpenShellClientFactory struct {
	client   openshellv1.ClientInterface
	gateways []appsv1alpha1.SandboxGatewaySpec
}

type serviceAwareFakeClient struct {
	openshellv1.ClientInterface
	services *fakeServiceClient
}

func (c *serviceAwareFakeClient) Services() openshellv1.ServiceInterface {
	return c.services
}

type fakeServiceClient struct {
	endpoints map[string]*openshellv1.ServiceEndpoint
}

func newFakeServiceClient() *fakeServiceClient {
	return &fakeServiceClient{endpoints: map[string]*openshellv1.ServiceEndpoint{}}
}

func serviceKey(workspace, sandboxName, serviceName string) string {
	return workspace + "/" + sandboxName + "/" + serviceName
}

func (c *fakeServiceClient) Expose(_ context.Context, workspace, sandboxName, serviceName string, targetPort uint32, domain bool) (*openshellv1.ServiceEndpoint, error) {
	endpoint := &openshellv1.ServiceEndpoint{
		ID:         "endpoint-" + serviceName,
		Sandbox:    sandboxName,
		Name:       serviceName,
		TargetPort: targetPort,
		Domain:     domain,
		URL:        fmt.Sprintf("https://%s-%s.example.test", sandboxName, serviceName),
		Workspace:  workspace,
	}
	c.endpoints[serviceKey(workspace, sandboxName, serviceName)] = endpoint
	copy := *endpoint
	return &copy, nil
}

func (c *fakeServiceClient) Get(_ context.Context, workspace, sandboxName, serviceName string) (*openshellv1.ServiceEndpoint, error) {
	endpoint, exists := c.endpoints[serviceKey(workspace, sandboxName, serviceName)]
	if !exists {
		return nil, &openshellv1.StatusError{Code: openshellv1.ErrorNotFound, Message: "service not found"}
	}
	copy := *endpoint
	return &copy, nil
}

func (c *fakeServiceClient) List(_, _ string, _ ...openshellv1.ListOptions) (*openshellv1.Pager[*openshellv1.ServiceEndpoint], error) {
	return nil, &openshellv1.StatusError{Code: openshellv1.ErrorUnimplemented, Message: "use ListAll"}
}

func (c *fakeServiceClient) ListAll(_ context.Context, workspace, sandboxName string, _ ...openshellv1.ListOptions) ([]*openshellv1.ServiceEndpoint, error) {
	result := []*openshellv1.ServiceEndpoint{}
	for _, endpoint := range c.endpoints {
		if endpoint.Workspace == workspace && endpoint.Sandbox == sandboxName {
			copy := *endpoint
			result = append(result, &copy)
		}
	}
	return result, nil
}

func (c *fakeServiceClient) Delete(_ context.Context, workspace, sandboxName, serviceName string, _ ...openshellv1.DeleteOptions) (*openshellv1.DeletionResult, error) {
	key := serviceKey(workspace, sandboxName, serviceName)
	if _, exists := c.endpoints[key]; !exists {
		return nil, &openshellv1.StatusError{Code: openshellv1.ErrorNotFound, Message: "service not found"}
	}
	delete(c.endpoints, key)
	return &openshellv1.DeletionResult{Outcome: openshellv1.DeletionCompleted}, nil
}

func (f *fakeOpenShellClientFactory) NewClient(gateway appsv1alpha1.SandboxGatewaySpec) (*OpenShellClientSession, error) {
	f.gateways = append(f.gateways, gateway)
	return &OpenShellClientSession{Client: f.client, Close: func() error { return nil }}, nil
}

var _ = Describe("SandboxRequest Controller", func() {
	const (
		resourceName = "test-sandbox-request"
		policyName   = "test-sandbox-policy"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: "default"}
	policyKey := types.NamespacedName{Name: policyName, Namespace: "default"}

	It("creates, replaces, observes, and deletes a sandbox through the SDK", func() {
		policy := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: "default"},
			Data: map[string]string{"policy.yaml": `version: 1
filesystem_policy:
  read_only: [/usr]
  read_write: [/sandbox]
`},
		}
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		gateway := appsv1alpha1.SandboxGatewaySpec{
			Endpoint: "http://openshell.openshell.svc.cluster.local:8080",
			Insecure: true,
		}
		request := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway:   gateway,
				Workspace: "pipelines",
				Image:     "quay.io/carewright/component:test",
				Command:   []string{"python", "-m", "component"},
				PolicyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: policyName},
					Key:                  "policy.yaml",
				},
				Providers: []string{"llm-credentials"},
				Env:       map[string]string{"LOG_LEVEL": "info", "PORT": "8080"},
				Resources: appsv1alpha1.SandboxResources{CPU: "500m", Memory: "512Mi"},
				Services: []appsv1alpha1.SandboxServiceSpec{{
					Name: "http", TargetPort: 8080,
				}},
				Labels: map[string]string{"app.kubernetes.io/component": "worker"},
			},
		}
		Expect(k8sClient.Create(ctx, request)).To(Succeed())

		sdkClient := openshellfake.NewClient()
		DeferCleanup(sdkClient.Close)
		serviceClient := newFakeServiceClient()
		factory := &fakeOpenShellClientFactory{client: &serviceAwareFakeClient{
			ClientInterface: sdkClient,
			services:        serviceClient,
		}}
		reconciler := &SandboxRequestReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			ClientFactory: factory,
		}

		By("adding the cleanup finalizer before creating an external resource")
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeTrue())
		Expect(factory.gateways).To(BeEmpty())

		By("creating the requested sandbox")
		result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(pollInterval))
		Expect(factory.gateways).To(ConsistOf(gateway, gateway))

		sandbox, err := sdkClient.Sandboxes().Get(ctx, "pipelines", resourceName)
		Expect(err).NotTo(HaveOccurred())
		Expect(sandbox.Spec.Template.Image).To(Equal("quay.io/carewright/component:test"))
		Expect(sandbox.Spec.Template.Resources).To(Equal(map[string]any{
			"limits": map[string]any{"cpu": "500m", "memory": "512Mi"},
		}))
		Expect(sandbox.Spec.Command).To(Equal([]string{"python", "-m", "component"}))
		Expect(sandbox.Spec.Environment).To(Equal(map[string]string{"LOG_LEVEL": "info", "PORT": "8080"}))
		Expect(sandbox.Spec.Providers).To(Equal([]string{"llm-credentials"}))
		Expect(sandbox.Spec.Policy.Version).To(Equal(uint32(1)))
		Expect(sandbox.Spec.Policy.Filesystem.ReadOnly).To(Equal([]string{"/usr"}))
		Expect(sandbox.Spec.Policy.Filesystem.ReadWrite).To(Equal([]string{"/sandbox"}))
		Expect(sandbox.Labels).To(HaveKeyWithValue("app.kubernetes.io/component", "worker"))
		Expect(sandbox.Labels["carewright.io/spec-hash"]).To(HaveLen(63))

		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.SandboxName).To(Equal(resourceName))
		Expect(request.Status.SpecHash).NotTo(BeEmpty())
		Expect(request.Status.ObservedGeneration).To(Equal(request.Generation))
		Expect(request.Status.Conditions).To(ContainElement(And(
			HaveField("Type", readyCondition),
			HaveField("Status", metav1.ConditionFalse),
		)))
		originalHash := request.Status.SpecHash

		By("replacing the sandbox when policy content changes")
		Expect(k8sClient.Get(ctx, policyKey, policy)).To(Succeed())
		policy.Data["policy.yaml"] = "version: 2\nfilesystem_policy:\n  read_write: [/sandbox, /tmp]\n"
		Expect(k8sClient.Update(ctx, policy)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.SpecHash).NotTo(Equal(originalHash))
		sandbox, err = sdkClient.Sandboxes().Get(ctx, "pipelines", resourceName)
		Expect(err).NotTo(HaveOccurred())
		Expect(sandbox.Spec.Policy.Version).To(Equal(uint32(2)))
		Expect(sandbox.Spec.Policy.Filesystem.ReadWrite).To(Equal([]string{"/sandbox", "/tmp"}))

		By("reporting a ready sandbox")
		_, err = sdkClient.Sandboxes().WaitReady(ctx, "pipelines", resourceName)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.Phase).To(Equal("Ready"))
		Expect(request.Status.Conditions).To(ContainElement(And(
			HaveField("Type", readyCondition),
			HaveField("Status", metav1.ConditionTrue),
		)))
		Expect(request.Status.Services).To(Equal([]appsv1alpha1.SandboxServiceStatus{{
			Name: "http", TargetPort: 8080, ID: "endpoint-http", URL: "https://test-sandbox-request-http.example.test",
		}}))

		By("replacing only the exposed endpoint when its target port changes")
		sandboxID := request.Status.SandboxID
		readyHash := request.Status.SpecHash
		request.Spec.Services[0].TargetPort = 9090
		Expect(k8sClient.Update(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.SandboxID).To(Equal(sandboxID))
		Expect(request.Status.SpecHash).To(Equal(readyHash))
		Expect(request.Status.Services).To(ContainElement(And(
			HaveField("Name", "http"),
			HaveField("TargetPort", int32(9090)),
		)))
		endpoint, err := serviceClient.Get(ctx, "pipelines", resourceName, "http")
		Expect(err).NotTo(HaveOccurred())
		Expect(endpoint.TargetPort).To(Equal(uint32(9090)))

		By("deleting an endpoint removed from the request")
		request.Spec.Services = nil
		Expect(k8sClient.Update(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.Services).To(BeEmpty())
		_, err = serviceClient.Get(ctx, "pipelines", resourceName, "http")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())

		By("deleting the external sandbox before removing the finalizer")
		Expect(k8sClient.Delete(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, key, &appsv1alpha1.SandboxRequest{})
			return apierrors.IsNotFound(err)
		}).Should(BeTrue())
		_, err = sdkClient.Sandboxes().Get(ctx, "pipelines", resourceName)
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())

		Expect(k8sClient.Delete(ctx, policy)).To(Succeed())
	})

	It("registers Secret-backed providers before creating a sandbox and rotates credentials", func() {
		const requestName = "provider-registration-test"
		const secretName = "provider-registration-credentials"
		requestKey := types.NamespacedName{Name: requestName, Namespace: "default"}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
			Data:       map[string][]byte{"api_key": []byte("first-key")},
		}
		request := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: requestName, Namespace: "default"},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway:   appsv1alpha1.SandboxGatewaySpec{Endpoint: "http://openshell.test:8080"},
				Workspace: "pipelines",
				Image:     "example.invalid/component:test",
				Providers: []string{"managed-llm"},
				ProviderRegistrations: []appsv1alpha1.SandboxProviderRegistration{{
					Name:      "managed-llm",
					Type:      "openai",
					SecretRef: corev1.LocalObjectReference{Name: secretName},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, request)).To(Succeed())

		sdkClient := openshellfake.NewClient()
		DeferCleanup(sdkClient.Close)
		reconciler := &SandboxRequestReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			ClientFactory: &fakeOpenShellClientFactory{client: sdkClient},
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())

		By("holding sandbox creation until the referenced Secret exists")
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).To(MatchError(ContainSubstring(secretName)))
		_, err = sdkClient.Sandboxes().Get(ctx, "pipelines", requestName)
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())

		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		provider, err := sdkClient.Providers().Get(ctx, "pipelines", "managed-llm")
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.Spec.Credentials).To(HaveKeyWithValue("api_key", "first-key"))
		Expect(provider.Annotations[providerSourceKey]).To(Equal("default/" + secretName))
		sandbox, err := sdkClient.Sandboxes().Get(ctx, "pipelines", requestName)
		Expect(err).NotTo(HaveOccurred())
		originalID := sandbox.ID

		By("updating the provider without replacing the sandbox on Secret rotation")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: secretName, Namespace: "default"}, secret)).To(Succeed())
		secret.Data["api_key"] = []byte("rotated-key")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		provider, err = sdkClient.Providers().Get(ctx, "pipelines", "managed-llm")
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.Spec.Credentials).To(HaveKeyWithValue("api_key", "rotated-key"))
		sandbox, err = sdkClient.Sandboxes().Get(ctx, "pipelines", requestName)
		Expect(err).NotTo(HaveOccurred())
		Expect(sandbox.ID).To(Equal(originalID))

		By("retaining a shared provider while another request still uses it")
		Expect(k8sClient.Get(ctx, requestKey, request)).To(Succeed())
		peerKey := types.NamespacedName{Name: "provider-registration-peer", Namespace: "default"}
		peer := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: peerKey.Name, Namespace: peerKey.Namespace},
			Spec:       *request.Spec.DeepCopy(),
		}
		Expect(k8sClient.Create(ctx, peer)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: peerKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: peerKey})
		Expect(err).NotTo(HaveOccurred())

		By("releasing a provider removed from the first request")
		request.Spec.Providers = []string{"replacement-llm"}
		request.Spec.ProviderRegistrations[0].Name = "replacement-llm"
		Expect(k8sClient.Update(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "managed-llm")
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "replacement-llm")
		Expect(err).NotTo(HaveOccurred())

		By("removing each provider after its last sandbox is deleted")
		Expect(k8sClient.Get(ctx, requestKey, request)).To(Succeed())
		Expect(k8sClient.Delete(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "replacement-llm")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "managed-llm")
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, peerKey, peer)).To(Succeed())
		Expect(k8sClient.Delete(ctx, peer)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: peerKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "managed-llm")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
	})

	It("keeps a finalizer until concurrent shared-provider deletions complete", func() {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "concurrent-provider-secret", Namespace: "default"},
			Data:       map[string][]byte{"api_key": []byte("test-key")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		sdkClient := openshellfake.NewClient()
		DeferCleanup(sdkClient.Close)
		reconciler := &SandboxRequestReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			ClientFactory: &fakeOpenShellClientFactory{client: sdkClient},
		}
		keys := []types.NamespacedName{
			{Name: "shared-provider-a", Namespace: "default"},
			{Name: "shared-provider-b", Namespace: "default"},
		}
		for _, item := range keys {
			request := &appsv1alpha1.SandboxRequest{
				ObjectMeta: metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace},
				Spec: appsv1alpha1.SandboxRequestSpec{
					Gateway:   appsv1alpha1.SandboxGatewaySpec{Endpoint: "http://openshell.test:8080"},
					Workspace: "pipelines",
					Image:     "example.invalid/component:test",
					Providers: []string{"shared-provider"},
					ProviderRegistrations: []appsv1alpha1.SandboxProviderRegistration{{
						Name:      "shared-provider",
						Type:      "openai",
						SecretRef: corev1.LocalObjectReference{Name: secret.Name},
					}},
				},
			}
			Expect(k8sClient.Create(ctx, request)).To(Succeed())
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: item})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: item})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, item, request)).To(Succeed())
			Expect(k8sClient.Delete(ctx, request)).To(Succeed())
		}

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: keys[1]})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Second))
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "shared-provider")
		Expect(err).NotTo(HaveOccurred())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: keys[0]})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: keys[1]})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "shared-provider")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
	})

	It("does not hand provider cleanup to a request without a finalizer", func() {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "unreconciled-peer-secret", Namespace: "default"},
			Data:       map[string][]byte{"api_key": []byte("test-key")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		ownerKey := types.NamespacedName{Name: "unreconciled-owner", Namespace: "default"}
		peerKey := types.NamespacedName{Name: "unreconciled-peer", Namespace: "default"}
		owner := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: ownerKey.Name, Namespace: ownerKey.Namespace},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway:   appsv1alpha1.SandboxGatewaySpec{Endpoint: "http://openshell.test:8080"},
				Workspace: "pipelines",
				Image:     "example.invalid/component:test",
				Providers: []string{"unreconciled-provider"},
				ProviderRegistrations: []appsv1alpha1.SandboxProviderRegistration{{
					Name: "unreconciled-provider", Type: "openai",
					SecretRef: corev1.LocalObjectReference{Name: secret.Name},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, owner)).To(Succeed())
		sdkClient := openshellfake.NewClient()
		DeferCleanup(sdkClient.Close)
		reconciler := &SandboxRequestReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			ClientFactory: &fakeOpenShellClientFactory{client: sdkClient},
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: ownerKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: ownerKey})
		Expect(err).NotTo(HaveOccurred())

		peer := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: peerKey.Name, Namespace: peerKey.Namespace},
			Spec:       *owner.Spec.DeepCopy(),
		}
		Expect(k8sClient.Create(ctx, peer)).To(Succeed())
		Expect(k8sClient.Get(ctx, ownerKey, owner)).To(Succeed())
		Expect(k8sClient.Delete(ctx, owner)).To(Succeed())
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: ownerKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Second))

		Expect(k8sClient.Delete(ctx, peer)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: ownerKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "unreconciled-provider")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
	})

	It("waits for a consumer-only sandbox before deleting its provider", func() {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "consumer-provider-secret", Namespace: "default"},
			Data:       map[string][]byte{"api_key": []byte("test-key")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		sdkClient := openshellfake.NewClient()
		DeferCleanup(sdkClient.Close)
		reconciler := &SandboxRequestReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			ClientFactory: &fakeOpenShellClientFactory{client: sdkClient},
		}
		managedKey := types.NamespacedName{Name: "consumer-provider-owner", Namespace: "default"}
		consumerKey := types.NamespacedName{Name: "consumer-provider-user", Namespace: "default"}
		gateway := appsv1alpha1.SandboxGatewaySpec{Endpoint: "http://openshell.test:8080"}
		managed := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: managedKey.Name, Namespace: managedKey.Namespace},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway: gateway, Workspace: "pipelines",
				Image:     "example.invalid/component:test",
				Providers: []string{"consumer-provider"},
				ProviderRegistrations: []appsv1alpha1.SandboxProviderRegistration{{
					Name: "consumer-provider", Type: "openai",
					SecretRef: corev1.LocalObjectReference{Name: secret.Name},
				}},
			},
		}
		consumer := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: consumerKey.Name, Namespace: consumerKey.Namespace},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway: gateway, Workspace: "pipelines",
				Image:     "example.invalid/component:test",
				Providers: []string{"consumer-provider"},
			},
		}
		for _, request := range []*appsv1alpha1.SandboxRequest{managed, consumer} {
			Expect(k8sClient.Create(ctx, request)).To(Succeed())
			key := types.NamespacedName{Name: request.Name, Namespace: request.Namespace}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(k8sClient.Get(ctx, managedKey, managed)).To(Succeed())
		managed.Spec.ProviderRegistrations = nil
		Expect(k8sClient.Update(ctx, managed)).To(Succeed())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: managedKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, managedKey, managed)).To(Succeed())
		Expect(managed.Status.ManagedProviders).To(HaveLen(1))
		Expect(k8sClient.Delete(ctx, managed)).To(Succeed())
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: managedKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Second))
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "consumer-provider")
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, consumerKey, consumer)).To(Succeed())
		Expect(k8sClient.Delete(ctx, consumer)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: consumerKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: managedKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "pipelines", "consumer-provider")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
	})

	It("cleans up the previous provider location after a workspace change", func() {
		const requestName = "provider-workspace-move"
		requestKey := types.NamespacedName{Name: requestName, Namespace: "default"}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "workspace-move-secret", Namespace: "default"},
			Data:       map[string][]byte{"api_key": []byte("test-key")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		request := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: requestName, Namespace: "default"},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway:   appsv1alpha1.SandboxGatewaySpec{Endpoint: "http://openshell.test:8080"},
				Workspace: "first",
				Image:     "example.invalid/component:test",
				Providers: []string{"moving-provider"},
				ProviderRegistrations: []appsv1alpha1.SandboxProviderRegistration{{
					Name:      "moving-provider",
					Type:      "openai",
					SecretRef: corev1.LocalObjectReference{Name: secret.Name},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, request)).To(Succeed())
		sdkClient := openshellfake.NewClient()
		DeferCleanup(sdkClient.Close)
		reconciler := &SandboxRequestReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			ClientFactory: &fakeOpenShellClientFactory{client: sdkClient},
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "first", "moving-provider")
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, requestKey, request)).To(Succeed())
		request.Spec.Workspace = "second"
		Expect(k8sClient.Update(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "first", "moving-provider")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		_, err = sdkClient.Providers().Get(ctx, "second", "moving-provider")
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, requestKey, request)).To(Succeed())
		Expect(k8sClient.Delete(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: requestKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = sdkClient.Providers().Get(ctx, "second", "moving-provider")
		Expect(openshellv1.IsNotFound(err)).To(BeTrue())
		Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
	})

	It("rejects unknown policy fields", func() {
		_, err := decodeSandboxPolicy([]byte("version: 1\nunknown_policy: true\n"))
		Expect(err).To(MatchError(ContainSubstring("unknown field")))
	})

	It("decodes CLI-style network policy YAML into SDK types", func() {
		policy, err := decodeSandboxPolicy([]byte(`version: 1
filesystem_policy:
  read_only: [/usr, /lib]
  read_write: [/sandbox, /tmp, /app]
network_policies:
  artifact-store:
    name: minio
    endpoints:
      - host: minio.default.svc.cluster.local
        port: 9000
        tls: skip
        enforcement: enforce
        access: full
        allowed_ips: [10.0.0.0/8]
    binaries:
      - path: "**"
`))
		Expect(err).NotTo(HaveOccurred())
		Expect(policy.Filesystem.ReadWrite).To(Equal([]string{"/sandbox", "/tmp", "/app"}))
		Expect(policy.NetworkPolicies).To(HaveKey("artifact-store"))
		rule := policy.NetworkPolicies["artifact-store"]
		Expect(rule.Name).To(Equal("minio"))
		Expect(rule.Endpoints).To(HaveLen(1))
		Expect(rule.Endpoints[0].Host).To(Equal("minio.default.svc.cluster.local"))
		Expect(rule.Endpoints[0].Port).To(Equal(uint32(9000)))
		Expect(rule.Endpoints[0].TLS).To(Equal(openshellv1.NetworkTLSModeSkip))
		Expect(rule.Endpoints[0].Enforcement).To(Equal(openshellv1.NetworkEnforcementModeEnforce))
		Expect(rule.Endpoints[0].Access).To(Equal(openshellv1.NetworkAccessPresetFull))
		Expect(rule.Endpoints[0].AllowedIPs).To(Equal([]string{"10.0.0.0/8"}))
		Expect(rule.Binaries).To(HaveLen(1))
		Expect(rule.Binaries[0].Path).To(Equal("**"))
	})

	It("merges first-class network access into a sandbox policy", func() {
		policy := mergeNetworkAccess(nil, []appsv1alpha1.SandboxNetworkAccessSpec{{
			Name: "sonataflow", Host: "pipeline.default.svc.cluster.local", Port: 443, Protocol: "https",
		}})
		Expect(policy.Version).To(Equal(uint32(1)))
		Expect(policy.NetworkPolicies).To(HaveKey("sonataflow"))
		rule := policy.NetworkPolicies["sonataflow"]
		Expect(rule.Endpoints).To(ConsistOf(openshellv1.PolicyNetworkEndpoint{
			Host: "pipeline.default.svc.cluster.local", Port: 443, Protocol: "rest", Enforcement: openshellv1.NetworkEnforcementModeEnforce, Access: openshellv1.NetworkAccessPresetFull,
		}))
		Expect(rule.Binaries).To(ConsistOf(openshellv1.PolicyNetworkBinary{Path: "**"}))
	})
})

func TestResolvedGatewayLocationSurvivesDefaultChange(t *testing.T) {
	t.Setenv("OPENSHELL_GATEWAY_ENDPOINT", "http://old-gateway.test:8080")
	request := &appsv1alpha1.SandboxRequest{
		Spec: appsv1alpha1.SandboxRequestSpec{
			Gateway:   appsv1alpha1.SandboxGatewaySpec{},
			Workspace: "default",
		},
		Status: appsv1alpha1.SandboxRequestStatus{
			SandboxName: "sandbox",
			Gateway:     resolvedGatewaySpec(appsv1alpha1.SandboxGatewaySpec{}),
			Workspace:   "default",
		},
	}
	if previousLocationChanged(request, "sandbox") {
		t.Fatal("the recorded gateway should match the current default")
	}
	t.Setenv("OPENSHELL_GATEWAY_ENDPOINT", "http://new-gateway.test:8080")
	if !previousLocationChanged(request, "sandbox") {
		t.Fatal("changing the default gateway must mark the old sandbox location as changed")
	}
	if request.Status.Gateway.Endpoint != "http://old-gateway.test:8080" {
		t.Fatalf("recorded gateway changed unexpectedly: %q", request.Status.Gateway.Endpoint)
	}
}

func TestDeletionComplete(t *testing.T) {
	cases := []struct {
		name   string
		result *openshellv1.DeletionResult
		want   bool
	}{
		{"nil", nil, false},
		{"unspecified", &openshellv1.DeletionResult{Outcome: openshellv1.DeletionUnspecified}, false},
		{"accepted", &openshellv1.DeletionResult{Outcome: openshellv1.DeletionAccepted}, false},
		{"completed", &openshellv1.DeletionResult{Outcome: openshellv1.DeletionCompleted}, true},
		{"already absent", &openshellv1.DeletionResult{Outcome: openshellv1.DeletionAlreadyAbsent}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deletionComplete(tc.result); got != tc.want {
				t.Fatalf("deletionComplete(%v) = %t, want %t", tc.result, got, tc.want)
			}
		})
	}
}
