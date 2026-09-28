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
	"maps"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"
)

// CarePlanWriterReconciler reconciles a CarePlanWriter object
type CarePlanWriterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.carewright.io,resources=careplanwriters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.carewright.io,resources=careplanwriters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.carewright.io,resources=careplanwriters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps.carewright.io,resources=sandboxrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sonataflow.org,resources=sonataflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=aistor.min.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete

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

	exposure, err := reconcileUIExposure(ctx, r.Client, r.Scheme, &writer)
	if err != nil {
		return ctrl.Result{}, err
	}

	artifactStore, err := reconcileArtifactStore(ctx, r.Client, &writer, writer.Spec.ArtifactStore)
	if err != nil {
		return ctrl.Result{}, err
	}
	effective := writer.DeepCopy()
	effective.Spec.ArtifactStore = artifactStore.Spec

	knownEndpoints, err := componentServiceURLs(ctx, r.Client, &writer)
	if err != nil {
		return ctrl.Result{}, err
	}

	template := pipelineWorkflowTemplate{
		WorkflowYAML: carePlanWriterWorkflowYAML,
		PropsYAML:    carePlanWriterPropsYAML,
		Replacements: map[string]string{
			"http://acp-patient-data:8080":    "patient-data",
			"http://acp-llm-reasoning:8080":   "llm-reasoning",
			"http://acp-decision-engine:8080": "decision-engine",
			"http://acp-fhir-generation:8080": "fhir-generation",
			"http://acp-fhir-server:8080":     "fhir-server",
			"http://acp-bff:8080":             "bff",
		},
	}
	components := carePlanWriterComponents(effective, knownEndpoints, workflowServiceURL(&writer))
	workflow, err := reconcilePipelineWorkflow(ctx, r.Client, r.Scheme, &writer, template, knownEndpoints, len(components))
	if err != nil {
		return ctrl.Result{}, err
	}
	if !workflow.Ready {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	if template.LiteralReplacements == nil {
		template.LiteralReplacements = map[string]string{}
	}
	template.LiteralReplacements[templateWorkflowURL(template.WorkflowYAML)] = strings.TrimRight(workflow.URL, "/")
	components = carePlanWriterComponents(effective, knownEndpoints, workflow.URL)

	summary, err := reconcileComponentSandboxes(ctx, r.Client, r.Scheme, &writer, writer.Spec.Sandbox, workflow.URL, components)
	if err != nil {
		return ctrl.Result{}, err
	}
	workflow, err = reconcilePipelineWorkflow(ctx, r.Client, r.Scheme, &writer, template, summary.Endpoints, summary.Desired)
	if err != nil {
		return ctrl.Result{}, err
	}
	before := writer.DeepCopy()
	writer.Status.ObservedGeneration = writer.Generation
	writer.Status.Endpoints = maps.Clone(summary.Endpoints)
	if artifactStore.URL != "" {
		writer.Status.Endpoints["artifactStore"] = artifactStore.URL
	}
	if workflow.Created {
		writer.Status.Endpoints["workflow"] = workflow.URL
	}
	if exposure.URL != "" {
		writer.Status.Endpoints["ui"] = exposure.URL
	}
	meta.SetStatusCondition(&writer.Status.Conditions, metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: writer.Generation,
		Reason:             "SpecAccepted",
		Message:            "The CarePlanWriter component sandboxes have been reconciled",
	})
	setPipelineReadyCondition(&writer.Status.Conditions, writer.Generation, summary, workflow)
	setUIExposureCondition(&writer.Status.Conditions, writer.Generation, exposure)
	if err := r.Status().Patch(ctx, &writer, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("reconciled CarePlanWriter sandboxes", "generation", writer.Generation, "ready", summary.Ready, "desired", summary.Desired)

	if exposure.URL == "" {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CarePlanWriterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.CarePlanWriter{}).
		Owns(&appsv1alpha1.SandboxRequest{}).
		Owns(sonataFlowObject()).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(routeObject()).
		Named("careplanwriter").
		Complete(r)
}

func carePlanWriterComponents(writer *appsv1alpha1.CarePlanWriter, endpoints map[string]string, workflowURL string) []sandboxComponent {
	artifactEnv, externalArtifactProvider := artifactStoreInputs(writer.Spec.ArtifactStore)
	var artifactSource *appsv1alpha1.ProviderSourceSpec
	if writer.Spec.ArtifactStore != nil {
		artifactSource = writer.Spec.ArtifactStore.Credentials
	}
	artifactProvider, artifactRegistration := managedProvider(writer, "artifact", externalArtifactProvider, artifactSource)
	artifactAccess := networkAccessForURL("artifact-store", artifactEnv["ARTIFACT_STORE_URL"])
	observabilityEnv := observabilityInputs(writer.Spec.Observability)
	llmEnv, externalLLMProvider := llmInputs(writer.Spec.LLM)
	var llmSource *appsv1alpha1.ProviderSourceSpec
	if writer.Spec.LLM != nil {
		llmSource = writer.Spec.LLM.Credentials
	}
	llmProvider, llmRegistration := managedProvider(writer, "llm", externalLLMProvider, llmSource)
	inferenceAccess := networkAccessForURL("inference", llmEnv["LITELLM_URL"])
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

	decisionServiceURL := endpoints["decision-service"]
	decisionEngineURL := endpoints["decision-engine"]
	llmReasoningURL := endpoints["llm-reasoning"]
	fhirServerURL := endpoints["fhir-server"]

	reasoningEnv := llmPythonEnv(writer.Spec.LLMReasoning.PythonComponentSpec)
	setIfNotEmpty(reasoningEnv, "DECISION_ENGINE_URL", decisionEngineURL)
	embeddingProvider := ""
	var embeddingRegistration *appsv1alpha1.SandboxProviderRegistration
	var embeddingAccess []appsv1alpha1.SandboxNetworkAccessSpec
	if embedding := writer.Spec.LLMReasoning.Embedding; embedding != nil {
		setIfNotEmpty(reasoningEnv, "EMBEDDING_PROVIDER", embedding.Provider)
		setIfNotEmpty(reasoningEnv, "EMBEDDING_MODEL", embedding.Model)
		setIfNotEmpty(reasoningEnv, "EMBEDDING_BASE_URL", embedding.URL)
		embeddingProvider = embedding.CredentialsProvider
		embeddingProvider, embeddingRegistration = managedProvider(writer, "embedding", embeddingProvider, embedding.Credentials)
		embeddingAccess = networkAccessForURL("embedding", embedding.URL)
	}

	decisionEnv := pythonEnv(writer.Spec.DecisionEngine)
	setIfNotEmpty(decisionEnv, "KOGITO_URL", decisionServiceURL)

	fhirGenerationEnv := llmPythonEnv(writer.Spec.FHIRGeneration)
	if transparency := writer.Spec.AITransparency; transparency != nil {
		fhirGenerationEnv["ACP_CAPTURE_PROMPTS"] = strconv.FormatBool(transparency.CapturePrompts)
		setIfNotEmpty(fhirGenerationEnv, "LLM_MODEL_CARD_URL", transparency.ModelCardURL)
	}

	fhirEnv := pythonEnv(writer.Spec.FHIRServer)
	fhirProvider := ""
	var fhirRegistration *appsv1alpha1.SandboxProviderRegistration
	var fhirTargetAccess []appsv1alpha1.SandboxNetworkAccessSpec
	if target := writer.Spec.FHIRTarget; target != nil {
		setIfNotEmpty(fhirEnv, "FHIR_SERVER_URL", target.URL)
		fhirProvider = target.CredentialsProvider
		fhirProvider, fhirRegistration = managedProvider(writer, "fhir", fhirProvider, target.Credentials)
		fhirTargetAccess = networkAccessForURL("fhir-target", target.URL)
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
	bffEnv["SONATAFLOW_URL"] = strings.TrimRight(workflowURL, "/")
	setIfNotEmpty(bffEnv, "LLM_REASONING_URL", llmReasoningURL)
	setIfNotEmpty(bffEnv, "DECISION_ENGINE_URL", decisionEngineURL)
	setIfNotEmpty(bffEnv, "FHIR_SERVER_URL", fhirServerURL)

	uiEnv := map[string]string{}
	setIfNotEmpty(uiEnv, "BFF_HOST", endpointHost(endpoints["bff"]))
	mcpEnv := llmPythonEnv(writer.Spec.MCP)
	setIfNotEmpty(mcpEnv, "KOGITO_URL", decisionServiceURL)
	setIfNotEmpty(mcpEnv, "FHIR_SERVER_URL", fhirServerURL)
	decisionServiceEnv := map[string]string{}
	setIfNotEmpty(decisionServiceEnv, "JAVA_OPTS_APPEND", writer.Spec.DecisionService.JavaOptions)

	return []sandboxComponent{
		{Name: "patient-data", Port: 8080, Spec: writer.Spec.PatientData.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.patient_data:app", "8080"), Env: pythonEnv(writer.Spec.PatientData), Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "llm-reasoning", Port: 8080, Spec: writer.Spec.LLMReasoning.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.llm_reasoning:app", "8080"), Env: reasoningEnv, Providers: []string{artifactProvider, llmProvider, embeddingProvider}, Registrations: providerRegistrations(artifactRegistration, llmRegistration, embeddingRegistration), NetworkAccess: combinedNetworkAccess(artifactAccess, inferenceAccess, embeddingAccess)},
		{Name: "decision-engine", Port: 8080, Spec: writer.Spec.DecisionEngine.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.decision_engine:app", "8080"), Env: decisionEnv, Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "fhir-generation", Port: 8080, Spec: writer.Spec.FHIRGeneration.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.fhir_generation:app", "8080"), Env: fhirGenerationEnv, Providers: []string{artifactProvider, llmProvider}, Registrations: providerRegistrations(artifactRegistration, llmRegistration), NetworkAccess: combinedNetworkAccess(artifactAccess, inferenceAccess)},
		{Name: "fhir-server", Port: 8080, Spec: writer.Spec.FHIRServer.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.fhir_server:app", "8080"), Env: fhirEnv, Providers: []string{artifactProvider, fhirProvider}, Registrations: providerRegistrations(artifactRegistration, fhirRegistration), NetworkAccess: combinedNetworkAccess(artifactAccess, fhirTargetAccess)},
		{Name: "bff", Port: 8080, Spec: writer.Spec.BFF.ComponentSpec, Command: pythonServiceCommand("acp_writer.services.bff:app", "8080"), Env: bffEnv, Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "ui", Port: 8080, Spec: writer.Spec.UI, Command: []string{"/usr/libexec/s2i/run"}, Env: uiEnv},
		{Name: "mcp", Port: 8090, Spec: writer.Spec.MCP.ComponentSpec, Command: pythonServiceCommand("acp_writer.mcp_proxy:app", "8090"), Env: mcpEnv, Providers: []string{llmProvider}, Registrations: providerRegistrations(llmRegistration), NetworkAccess: inferenceAccess},
		{Name: "decision-service", Port: 8081, Spec: writer.Spec.DecisionService.ComponentSpec, Command: []string{"java", "-jar", "/app/quarkus-run.jar"}, Env: decisionServiceEnv},
	}
}
