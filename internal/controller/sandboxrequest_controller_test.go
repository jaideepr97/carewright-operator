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
	"errors"
	"os"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "cpgtoacp.io/cpgtoacp-operator/api/v1alpha1"
)

type fakeOpenShellRunner struct {
	exists      bool
	calls       [][]string
	createCalls [][]string
}

func (f *fakeOpenShellRunner) Run(_ context.Context, args ...string) ([]byte, error) {
	copyArgs := slices.Clone(args)
	f.calls = append(f.calls, copyArgs)

	switch {
	case len(args) >= 2 && args[0] == "sandbox" && args[1] == "get":
		if !f.exists {
			return nil, &OpenShellCommandError{Output: "sandbox not found", Err: errors.New("exit status 1")}
		}
		return []byte(`{"sandbox":{"id":"sandbox-123","state":"running"}}`), nil
	case len(args) >= 2 && args[0] == "sandbox" && args[1] == "create":
		f.exists = true
		f.createCalls = append(f.createCalls, copyArgs)
		policyIndex := slices.Index(args, "--policy")
		Expect(policyIndex).To(BeNumerically(">=", 0))
		_, err := os.Stat(args[policyIndex+1])
		Expect(err).NotTo(HaveOccurred())
		return []byte(`{"sandbox":{"id":"sandbox-123","state":"creating"}}`), nil
	case len(args) >= 2 && args[0] == "sandbox" && args[1] == "delete":
		if !f.exists {
			return nil, &OpenShellCommandError{Output: "sandbox not found", Err: errors.New("exit status 1")}
		}
		f.exists = false
		return nil, nil
	default:
		return nil, errors.New("unexpected OpenShell invocation")
	}
}

var _ = Describe("SandboxRequest Controller", func() {
	const (
		resourceName = "test-sandbox-request"
		policyName   = "test-sandbox-policy"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: "default"}
	policyKey := types.NamespacedName{Name: policyName, Namespace: "default"}

	It("creates, replaces, observes, and deletes a sandbox", func() {
		policy := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: "default"},
			Data:       map[string]string{"policy.yaml": "version: 1\n"},
		}
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		request := &appsv1alpha1.SandboxRequest{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
			Spec: appsv1alpha1.SandboxRequestSpec{
				Gateway:   appsv1alpha1.SandboxGatewaySpec{Name: "local-gateway"},
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
				Labels:    map[string]string{"app.kubernetes.io/component": "worker"},
			},
		}
		Expect(k8sClient.Create(ctx, request)).To(Succeed())

		runner := &fakeOpenShellRunner{}
		reconciler := &SandboxRequestReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Runner: runner}

		By("adding the cleanup finalizer before creating an external resource")
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Requeue).To(BeTrue())
		Expect(runner.calls).To(BeEmpty())

		By("creating the requested sandbox")
		result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(pollInterval))
		Expect(runner.createCalls).To(HaveLen(1))
		createArgs := runner.createCalls[0]
		Expect(createArgs).To(ContainElements(
			"--name", resourceName,
			"--from", "quay.io/cpgtoacp/component:test",
			"--gateway", "local-gateway",
			"--workspace", "pipelines",
			"--provider", "llm-credentials",
			"--env", "LOG_LEVEL=info",
			"--cpu", "500m",
			"--memory", "512Mi",
		))
		Expect(strings.Join(createArgs, " ")).To(ContainSubstring("-- python -m component"))
		Expect(createArgs).NotTo(ContainElement("--output"))
		specHashLabel := ""
		for index, arg := range createArgs {
			if arg == "--label" && index+1 < len(createArgs) && strings.HasPrefix(createArgs[index+1], "cpgtoacp.io/spec-hash=") {
				specHashLabel = strings.TrimPrefix(createArgs[index+1], "cpgtoacp.io/spec-hash=")
			}
		}
		Expect(specHashLabel).To(HaveLen(63))

		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.SandboxName).To(Equal(resourceName))
		Expect(request.Status.SandboxID).To(Equal("sandbox-123"))
		Expect(request.Status.SpecHash).NotTo(BeEmpty())
		Expect(request.Status.ObservedGeneration).To(Equal(request.Generation))
		Expect(request.Status.Conditions).To(ContainElement(And(
			HaveField("Type", readyCondition),
			HaveField("Status", metav1.ConditionFalse),
		)))
		originalHash := request.Status.SpecHash

		By("replacing the sandbox when policy content changes")
		Expect(k8sClient.Get(ctx, policyKey, policy)).To(Succeed())
		policy.Data["policy.yaml"] = "version: 1\nnetwork_policies: {}\n"
		Expect(k8sClient.Update(ctx, policy)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(runner.createCalls).To(HaveLen(2))
		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.SpecHash).NotTo(Equal(originalHash))

		By("reporting a running sandbox as ready")
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, request)).To(Succeed())
		Expect(request.Status.Phase).To(Equal("running"))
		Expect(request.Status.Conditions).To(ContainElement(And(
			HaveField("Type", readyCondition),
			HaveField("Status", metav1.ConditionTrue),
		)))

		By("deleting the external sandbox before removing the finalizer")
		Expect(k8sClient.Delete(ctx, request)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, key, &appsv1alpha1.SandboxRequest{})
			return apierrors.IsNotFound(err)
		}).Should(BeTrue())

		Expect(k8sClient.Delete(ctx, policy)).To(Succeed())
	})
})
