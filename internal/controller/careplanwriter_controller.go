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
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	appsv1alpha1 "cpgtoacp.io/cpgtoacp-operator/api/v1alpha1"
)

// CarePlanWriterReconciler reconciles a CarePlanWriter object
type CarePlanWriterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=careplanwriters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=careplanwriters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=careplanwriters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=sandboxrequests,verbs=get;list;watch;create;update;patch;delete

// Reconcile creates and manages one SandboxRequest for each Care Plan Writer component.
func (r *CarePlanWriterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var writer appsv1alpha1.CarePlanWriter
	if err := r.Get(ctx, req.NamespacedName, &writer); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	summary, err := reconcileComponentSandboxes(
		ctx,
		r.Client,
		r.Scheme,
		&writer,
		writer.Spec.Sandbox,
		carePlanWriterComponents(&writer),
	)
	if err != nil {
		return ctrl.Result{}, err
	}

	before := writer.DeepCopy()
	writer.Status.ObservedGeneration = writer.Generation
	meta.SetStatusCondition(&writer.Status.Conditions, metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: writer.Generation,
		Reason:             "SpecAccepted",
		Message:            "The CarePlanWriter component sandboxes have been reconciled",
	})
	setSandboxReadyCondition(&writer.Status.Conditions, writer.Generation, summary)
	if err := r.Status().Patch(ctx, &writer, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("reconciled CarePlanWriter sandboxes", "generation", writer.Generation, "ready", summary.Ready, "desired", summary.Desired)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CarePlanWriterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.CarePlanWriter{}).
		Owns(&appsv1alpha1.SandboxRequest{}).
		Named("careplanwriter").
		Complete(r)
}

func carePlanWriterComponents(writer *appsv1alpha1.CarePlanWriter) []sandboxComponent {
	artifactEnv, artifactProvider := artifactStoreInputs(writer.Spec.ArtifactStore)
	observabilityEnv := observabilityInputs(writer.Spec.Observability)
	llmEnv, llmProvider := llmInputs(writer.Spec.LLM)
	baseEnv := mergedEnv(artifactEnv, observabilityEnv)

	pythonEnv := func(spec appsv1alpha1.PythonComponentSpec) map[string]string {
		env := mergedEnv(baseEnv, nil)
		setIfNotEmpty(env, "PYTHONPATH", spec.PythonPath)
		return env
	}
	llmPythonEnv := func(spec appsv1alpha1.PythonComponentSpec) map[string]string {
		env := pythonEnv(spec)
		return mergedEnv(env, llmEnv)
	}

	decisionServiceURL := "http://" + componentRequestName(writer.Name, "decision-service") + ":8081"
	decisionEngineURL := "http://" + componentRequestName(writer.Name, "decision-engine") + ":8080"
	llmReasoningURL := "http://" + componentRequestName(writer.Name, "llm-reasoning") + ":8080"
	fhirServerURL := "http://" + componentRequestName(writer.Name, "fhir-server") + ":8080"

	reasoningEnv := llmPythonEnv(writer.Spec.LLMReasoning.PythonComponentSpec)
	reasoningEnv["DECISION_ENGINE_URL"] = decisionEngineURL
	embeddingProvider := ""
	if embedding := writer.Spec.LLMReasoning.Embedding; embedding != nil {
		setIfNotEmpty(reasoningEnv, "EMBEDDING_PROVIDER", embedding.Provider)
		setIfNotEmpty(reasoningEnv, "EMBEDDING_MODEL", embedding.Model)
		setIfNotEmpty(reasoningEnv, "EMBEDDING_BASE_URL", embedding.URL)
		embeddingProvider = embedding.CredentialsProvider
	}

	decisionEnv := pythonEnv(writer.Spec.DecisionEngine)
	decisionEnv["KOGITO_URL"] = decisionServiceURL

	fhirGenerationEnv := llmPythonEnv(writer.Spec.FHIRGeneration)
	if transparency := writer.Spec.AITransparency; transparency != nil {
		fhirGenerationEnv["ACP_CAPTURE_PROMPTS"] = strconv.FormatBool(transparency.CapturePrompts)
		setIfNotEmpty(fhirGenerationEnv, "LLM_MODEL_CARD_URL", transparency.ModelCardURL)
	}

	fhirEnv := pythonEnv(writer.Spec.FHIRServer)
	fhirProvider := ""
	if target := writer.Spec.FHIRTarget; target != nil {
		setIfNotEmpty(fhirEnv, "FHIR_SERVER_URL", target.URL)
		fhirProvider = target.CredentialsProvider
	}
	if transparency := writer.Spec.AITransparency; transparency != nil && transparency.Reviewer != nil {
		reviewer := transparency.Reviewer
		setIfNotEmpty(fhirEnv, "ACP_REVIEWER_DISPLAY", reviewer.Display)
		setIfNotEmpty(fhirEnv, "ACP_REVIEWER_REFERENCE", reviewer.Reference)
		setIfNotEmpty(fhirEnv, "ACP_REVIEWER_ID_SYSTEM", reviewer.IdentifierSystem)
		setIfNotEmpty(fhirEnv, "ACP_REVIEWER_ID_VALUE", reviewer.IdentifierValue)
	}

	bffEnv := pythonEnv(writer.Spec.BFF)
	if writer.Spec.ArtifactStore != nil {
		setIfNotEmpty(bffEnv, "MINIO_ENDPOINT", writer.Spec.ArtifactStore.URL)
	}
	bffEnv["LLM_REASONING_URL"] = llmReasoningURL
	bffEnv["DECISION_ENGINE_URL"] = decisionEngineURL
	bffEnv["FHIR_SERVER_URL"] = fhirServerURL

	uiEnv := map[string]string{"BFF_HOST": componentRequestName(writer.Name, "bff") + ":8080"}
	mcpEnv := llmPythonEnv(writer.Spec.MCP)
	mcpEnv["KOGITO_URL"] = decisionServiceURL
	mcpEnv["FHIR_SERVER_URL"] = fhirServerURL
	decisionServiceEnv := map[string]string{}
	setIfNotEmpty(decisionServiceEnv, "JAVA_OPTS_APPEND", writer.Spec.DecisionService.JavaOptions)

	return []sandboxComponent{
		{Name: "patient-data", Port: 8080, Spec: writer.Spec.PatientData.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.patient_data:app", "8080"), Env: pythonEnv(writer.Spec.PatientData), Providers: []string{artifactProvider}},
		{Name: "llm-reasoning", Port: 8080, Spec: writer.Spec.LLMReasoning.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.llm_reasoning:app", "8080"), Env: reasoningEnv, Providers: []string{artifactProvider, llmProvider, embeddingProvider}},
		{Name: "decision-engine", Port: 8080, Spec: writer.Spec.DecisionEngine.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.decision_engine:app", "8080"), Env: decisionEnv, Providers: []string{artifactProvider}},
		{Name: "fhir-generation", Port: 8080, Spec: writer.Spec.FHIRGeneration.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.fhir_generation:app", "8080"), Env: fhirGenerationEnv, Providers: []string{artifactProvider, llmProvider}},
		{Name: "fhir-server", Port: 8080, Spec: writer.Spec.FHIRServer.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.fhir_server:app", "8080"), Env: fhirEnv, Providers: []string{artifactProvider, fhirProvider}},
		{Name: "bff", Port: 8080, Spec: writer.Spec.BFF.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.bff:app", "8080"), Env: bffEnv, Providers: []string{artifactProvider}},
		{Name: "ui", Port: 8080, Spec: writer.Spec.UI, Command: []string{"/usr/libexec/s2i/run"}, Env: uiEnv},
		{Name: "mcp", Port: 8090, Spec: writer.Spec.MCP.ComponentSpec, Command: pythonServiceCommand("acp_writer.mcp_proxy:app", "8090"), Env: mcpEnv, Providers: []string{llmProvider}},
		{Name: "decision-service", Port: 8081, Spec: writer.Spec.DecisionService.ComponentSpec, Command: []string{"java", "-jar", "/app/quarkus-run.jar"}, Env: decisionServiceEnv},
	}
}
