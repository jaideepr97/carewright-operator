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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("CPGIngester Controller", func() {
	const resourceName = "cpgingester-controller-test"
	resourceKey := types.NamespacedName{Name: resourceName, Namespace: "default"}

	AfterEach(func() {
		var requests appsv1alpha1.SandboxRequestList
		Expect(k8sClient.List(ctx, &requests, client.InNamespace("default"))).To(Succeed())
		for index := range requests.Items {
			if requests.Items[index].Labels[pipelineLabel] != "" {
				Expect(k8sClient.Delete(ctx, &requests.Items[index])).To(Succeed())
			}
		}
		resource := &appsv1alpha1.CPGIngester{}
		if err := k8sClient.Get(ctx, resourceKey, resource); err == nil {
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		}
	})

	It("creates, updates, and reports readiness for component SandboxRequests", func() {
		image := func(name string) appsv1alpha1.ComponentSpec {
			return appsv1alpha1.ComponentSpec{Image: "example.invalid/" + name + ":latest"}
		}
		resource := &appsv1alpha1.CPGIngester{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
			Spec: appsv1alpha1.CPGIngesterSpec{
				Sandbox: appsv1alpha1.PipelineSandboxSpec{
					Gateway:   appsv1alpha1.SandboxGatewaySpec{Endpoint: "http://openshell.test:8080", Insecure: true},
					Workspace: "pipelines",
				},
				ArtifactStore: &appsv1alpha1.ArtifactStoreSpec{
					URL:                 "http://minio.test:9000",
					ArtifactBucket:      "artifacts",
					CredentialsProvider: "minio-provider",
				},
				Observability: &appsv1alpha1.ObservabilitySpec{MLflowTrackingURI: "http://mlflow.test:5000"},
				LLM: &appsv1alpha1.LLMConfigSpec{
					URL:                   "http://litellm.test:4000",
					Model:                 "test-model",
					CredentialsProvider:   "llm-provider",
					RequestTimeoutSeconds: 30,
				},
				Ingestion: appsv1alpha1.CPGIngestionComponentSpec{ComponentSpec: image("ingestion"), OCREnabled: true},
				LLMAnalysis: appsv1alpha1.CPGLLMAnalysisComponentSpec{
					PythonComponentSpec:            appsv1alpha1.PythonComponentSpec{ComponentSpec: image("llm"), PythonPath: "/app/src"},
					FigureInterpretationEnabled:    true,
					FigureInterpretationMaxFigures: 12,
				},
				Assembly: appsv1alpha1.PythonComponentSpec{
					ComponentSpec: appsv1alpha1.ComponentSpec{
						Image:          "example.invalid/assembly:v1",
						Command:        []string{"custom-assembly"},
						PolicyRef:      &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "assembly-policy"}, Key: "policy.yaml"},
						Resources:      appsv1alpha1.SandboxResources{CPU: "200m", Memory: "512Mi"},
						ExtraProviders: []string{"extra-provider", "minio-provider"},
						ExtraEnv:       map[string]string{"CUSTOM": "value", "PYTHONPATH": "ignored"},
					},
					PythonPath: "/app/src",
				},
				Delivery: appsv1alpha1.PythonComponentSpec{ComponentSpec: image("delivery")},
				BFF:      appsv1alpha1.PythonComponentSpec{ComponentSpec: image("bff")},
				UI:       image("ui"),
			},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		reconciler := &CPGIngesterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: resourceKey})
		Expect(err).NotTo(HaveOccurred())

		var requests appsv1alpha1.SandboxRequestList
		Expect(k8sClient.List(ctx, &requests, client.InNamespace("default"), client.MatchingLabels{pipelineLabel: string(resource.UID)})).To(Succeed())
		Expect(requests.Items).To(HaveLen(6))

		assembly := &appsv1alpha1.SandboxRequest{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-assembly", Namespace: "default"}, assembly)).To(Succeed())
		Expect(len(assembly.Spec.SandboxName)).To(BeNumerically("<=", 19))
		Expect(assembly.Spec.SandboxName).To(Equal(componentSandboxName(resourceName, "assembly")))
		Expect(assembly.Spec.Gateway.Endpoint).To(Equal("http://openshell.test:8080"))
		Expect(assembly.Spec.Workspace).To(Equal("pipelines"))
		Expect(assembly.Spec.Image).To(Equal("example.invalid/assembly:v1"))
		Expect(assembly.Spec.Command).To(Equal([]string{"custom-assembly"}))
		Expect(assembly.Spec.PolicyRef.Name).To(Equal("assembly-policy"))
		Expect(assembly.Spec.Resources.Memory).To(Equal("512Mi"))
		Expect(assembly.Spec.Services).To(Equal([]appsv1alpha1.SandboxServiceSpec{{Name: "http", TargetPort: 8080}}))
		Expect(assembly.Spec.NetworkAccess).To(ConsistOf(appsv1alpha1.SandboxNetworkAccessSpec{
			Name: "sonataflow", Host: workflowServiceHost(resource), Port: 80, Protocol: "rest",
		}))
		Expect(assembly.Spec.Providers).To(ConsistOf("minio-provider", "extra-provider"))
		Expect(assembly.Spec.Env).To(HaveKeyWithValue("ARTIFACT_STORE_URL", "http://minio.test:9000"))
		Expect(assembly.Spec.Env).To(HaveKeyWithValue("MLFLOW_TRACKING_URI", "http://mlflow.test:5000"))
		Expect(assembly.Spec.Env).To(HaveKeyWithValue("PYTHONPATH", "/app/src"))
		Expect(assembly.Spec.Env).To(HaveKeyWithValue("CUSTOM", "value"))
		Expect(metav1.IsControlledBy(assembly, resource)).To(BeTrue())

		analysis := &appsv1alpha1.SandboxRequest{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-llm-analysis", Namespace: "default"}, analysis)).To(Succeed())
		Expect(analysis.Spec.Providers).To(ConsistOf("minio-provider", "llm-provider"))
		Expect(analysis.Spec.Env).To(HaveKeyWithValue("LITELLM_URL", "http://litellm.test:4000"))
		Expect(analysis.Spec.Env).To(HaveKeyWithValue("FIGURE_INTERPRETATION_MAX_FIGURES", "12"))
		bff := &appsv1alpha1.SandboxRequest{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-bff", Namespace: "default"}, bff)).To(Succeed())
		Expect(bff.Spec.Env).To(HaveKeyWithValue("SONATAFLOW_URL", workflowServiceURL(resource)))

		updated := &appsv1alpha1.CPGIngester{}
		Expect(k8sClient.Get(ctx, resourceKey, updated)).To(Succeed())
		Expect(updated.Status.ObservedGeneration).To(Equal(updated.Generation))
		Expect(meta.IsStatusConditionTrue(updated.Status.Conditions, readyCondition)).To(BeFalse())

		for index := range requests.Items {
			request := &requests.Items[index]
			request.Status.ObservedGeneration = request.Generation
			request.Status.Services = []appsv1alpha1.SandboxServiceStatus{{
				Name: "http", TargetPort: request.Spec.Services[0].TargetPort, URL: "https://" + request.Labels[componentLabel] + ".gateway.test",
			}}
			meta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{
				Type: readyCondition, Status: metav1.ConditionTrue, Reason: "Ready", ObservedGeneration: request.Generation,
			})
			Expect(k8sClient.Status().Update(ctx, request)).To(Succeed())
		}
		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: resourceKey})
		Expect(err).NotTo(HaveOccurred())

		workflow := sonataFlowObject()
		workflowKey := types.NamespacedName{Name: workflowResourceName(resource), Namespace: "default"}
		Expect(k8sClient.Get(ctx, workflowKey, workflow)).To(Succeed())
		workflowJSON, err := workflow.MarshalJSON()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(workflowJSON)).To(ContainSubstring("https://assembly.gateway.test/api/v1/assemble"))
		Expect(string(workflowJSON)).To(ContainSubstring(workflowServiceURL(resource) + "/wait-parse"))
		props := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: workflowResourceName(resource) + "-props", Namespace: "default"}, props)).To(Succeed())
		Expect(props.Data["application.properties"]).To(ContainSubstring("mp.messaging.incoming.parse-done.path=/wait-parse"))

		Expect(k8sClient.Get(ctx, resourceKey, updated)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(updated.Status.Conditions, readyCondition)).To(BeFalse())
		Expect(updated.Status.Endpoints).To(HaveKeyWithValue("assembly", "https://assembly.gateway.test"))
		Expect(updated.Status.Endpoints).To(HaveKeyWithValue("workflow", workflowServiceURL(resource)))

		Expect(k8sClient.List(ctx, &requests, client.InNamespace("default"), client.MatchingLabels{pipelineLabel: string(resource.UID)})).To(Succeed())
		for index := range requests.Items {
			request := &requests.Items[index]
			request.Status.ObservedGeneration = request.Generation
			meta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{
				Type: readyCondition, Status: metav1.ConditionTrue, Reason: "Ready", ObservedGeneration: request.Generation,
			})
			Expect(k8sClient.Status().Update(ctx, request)).To(Succeed())
		}
		workflow.Object["status"] = map[string]any{
			"observedGeneration": workflow.GetGeneration(),
			"address":            map[string]any{"url": "http://sonataflow-address.test"},
			"conditions": []any{map[string]any{
				"type": "Running", "status": "True",
			}},
		}
		Expect(k8sClient.Status().Update(ctx, workflow)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: resourceKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, resourceKey, updated)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(updated.Status.Conditions, readyCondition)).To(BeTrue())
		Expect(updated.Status.Endpoints).To(HaveKeyWithValue("workflow", "http://sonataflow-address.test"))

		updated.Spec.Assembly.Image = "example.invalid/assembly:v2"
		Expect(k8sClient.Update(ctx, updated)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: resourceKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: assembly.Name, Namespace: "default"}, assembly)).To(Succeed())
		Expect(assembly.Spec.Image).To(Equal("example.invalid/assembly:v2"))
	})
})
