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

	openshellv1 "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	openshellfake "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1/fake"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "cpgtoacp.io/cpgtoacp-operator/api/v1alpha1"
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
		ID:          "endpoint-" + serviceName,
		SandboxName: sandboxName,
		ServiceName: serviceName,
		TargetPort:  targetPort,
		Domain:      domain,
		URL:         fmt.Sprintf("https://%s-%s.example.test", sandboxName, serviceName),
		Workspace:   workspace,
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

func (c *fakeServiceClient) List(_ context.Context, workspace, sandboxName string, _ ...openshellv1.ListOptions) ([]*openshellv1.ServiceEndpoint, error) {
	result := []*openshellv1.ServiceEndpoint{}
	for _, endpoint := range c.endpoints {
		if endpoint.Workspace == workspace && endpoint.SandboxName == sandboxName {
			copy := *endpoint
			result = append(result, &copy)
		}
	}
	return result, nil
}

func (c *fakeServiceClient) Delete(_ context.Context, workspace, sandboxName, serviceName string) error {
	key := serviceKey(workspace, sandboxName, serviceName)
	if _, exists := c.endpoints[key]; !exists {
		return &openshellv1.StatusError{Code: openshellv1.ErrorNotFound, Message: "service not found"}
	}
	delete(c.endpoints, key)
	return nil
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
				Image:     "quay.io/cpgtoacp/component:test",
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
		Expect(sandbox.Spec.Template.Image).To(Equal("quay.io/cpgtoacp/component:test"))
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
		Expect(sandbox.Labels["cpgtoacp.io/spec-hash"]).To(HaveLen(63))

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
        enforcement: enforce
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
		Expect(rule.Endpoints[0].AllowedIPs).To(Equal([]string{"10.0.0.0/8"}))
		Expect(rule.Binaries).To(HaveLen(1))
		Expect(rule.Binaries[0].Path).To(Equal("**"))
	})
})
