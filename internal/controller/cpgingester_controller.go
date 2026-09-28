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

// CPGIngesterReconciler reconciles a CPGIngester object
type CPGIngesterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.carewright.io,resources=cpgingesters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.carewright.io,resources=cpgingesters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.carewright.io,resources=cpgingesters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps.carewright.io,resources=sandboxrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sonataflow.org,resources=sonataflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=aistor.min.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete

// Reconcile creates and manages one SandboxRequest for each CPG Ingester component.
func (r *CPGIngesterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var ingester appsv1alpha1.CPGIngester
	if err := r.Get(ctx, req.NamespacedName, &ingester); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	exposure, err := reconcileUIExposure(ctx, r.Client, r.Scheme, &ingester)
	if err != nil {
		return ctrl.Result{}, err
	}

	artifactStore, err := reconcileArtifactStore(ctx, r.Client, &ingester, ingester.Spec.ArtifactStore)
	if err != nil {
		return ctrl.Result{}, err
	}
	effective := ingester.DeepCopy()
	effective.Spec.ArtifactStore = artifactStore.Spec

	knownEndpoints, err := componentServiceURLs(ctx, r.Client, &ingester)
	if err != nil {
		return ctrl.Result{}, err
	}

	literals := map[string]string{}
	if ingester.Spec.CarePlanWriterRef != nil {
		var writer appsv1alpha1.CarePlanWriter
		if err := r.Get(ctx, client.ObjectKey{Name: ingester.Spec.CarePlanWriterRef.Name, Namespace: ingester.Namespace}, &writer); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		} else if err == nil && writer.Status.Endpoints["bff"] != "" {
			literals["http://acp-bff:8080"] = strings.TrimRight(writer.Status.Endpoints["bff"], "/")
		}
	}
	template := pipelineWorkflowTemplate{
		WorkflowYAML: cpgIngesterWorkflowYAML,
		PropsYAML:    cpgIngesterPropsYAML,
		Replacements: map[string]string{
			"http://cpg-ingester-ingestion:8080":    "ingestion",
			"http://cpg-ingester-llm-analysis:8080": "llm-analysis",
			"http://cpg-ingester-assembly:8080":     "assembly",
			"http://cpg-ingester-delivery:8080":     "delivery",
			"http://cpg-ingester-bff:8080":          "bff",
		},
		LiteralReplacements: literals,
	}
	components := cpgIngesterComponents(effective, knownEndpoints, workflowServiceURL(&ingester))
	workflow, err := reconcilePipelineWorkflow(ctx, r.Client, r.Scheme, &ingester, template, knownEndpoints, len(components))
	if err != nil {
		return ctrl.Result{}, err
	}
	if !workflow.Ready {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	template.LiteralReplacements[templateWorkflowURL(template.WorkflowYAML)] = strings.TrimRight(workflow.URL, "/")
	components = cpgIngesterComponents(effective, knownEndpoints, workflow.URL)

	summary, err := reconcileComponentSandboxes(ctx, r.Client, r.Scheme, &ingester, ingester.Spec.Sandbox, workflow.URL, components)
	if err != nil {
		return ctrl.Result{}, err
	}
	workflow, err = reconcilePipelineWorkflow(ctx, r.Client, r.Scheme, &ingester, template, summary.Endpoints, summary.Desired)
	if err != nil {
		return ctrl.Result{}, err
	}
	before := ingester.DeepCopy()
	ingester.Status.ObservedGeneration = ingester.Generation
	ingester.Status.Endpoints = maps.Clone(summary.Endpoints)
	if artifactStore.URL != "" {
		ingester.Status.Endpoints["artifactStore"] = artifactStore.URL
	}
	if workflow.Created {
		ingester.Status.Endpoints["workflow"] = workflow.URL
	}
	if exposure.URL != "" {
		ingester.Status.Endpoints["ui"] = exposure.URL
	}
	meta.SetStatusCondition(&ingester.Status.Conditions, metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ingester.Generation,
		Reason:             "SpecAccepted",
		Message:            "The CPGIngester component sandboxes have been reconciled",
	})
	setPipelineReadyCondition(&ingester.Status.Conditions, ingester.Generation, summary, workflow)
	setUIExposureCondition(&ingester.Status.Conditions, ingester.Generation, exposure)
	if err := r.Status().Patch(ctx, &ingester, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("reconciled CPGIngester sandboxes", "generation", ingester.Generation, "ready", summary.Ready, "desired", summary.Desired)

	if exposure.URL == "" {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CPGIngesterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.CPGIngester{}).
		Owns(&appsv1alpha1.SandboxRequest{}).
		Owns(sonataFlowObject()).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(routeObject()).
		Named("cpgingester").
		Complete(r)
}

func cpgIngesterComponents(ingester *appsv1alpha1.CPGIngester, endpoints map[string]string, workflowURL string) []sandboxComponent {
	artifactEnv, externalArtifactProvider := artifactStoreInputs(ingester.Spec.ArtifactStore)
	var artifactSource *appsv1alpha1.ProviderSourceSpec
	if ingester.Spec.ArtifactStore != nil {
		artifactSource = ingester.Spec.ArtifactStore.Credentials
	}
	artifactProvider, artifactRegistration := managedProvider(ingester, "artifact", externalArtifactProvider, artifactSource)
	artifactAccess := networkAccessForURL("artifact-store", artifactEnv["ARTIFACT_STORE_URL"])
	observabilityEnv := observabilityInputs(ingester.Spec.Observability)
	llmEnv, externalLLMProvider := llmInputs(ingester.Spec.LLM)
	var llmSource *appsv1alpha1.ProviderSourceSpec
	if ingester.Spec.LLM != nil {
		llmSource = ingester.Spec.LLM.Credentials
	}
	llmProvider, llmRegistration := managedProvider(ingester, "llm", externalLLMProvider, llmSource)
	inferenceAccess := networkAccessForURL("inference", llmEnv["LITELLM_URL"])

	ingestionEnv := mergedEnv(artifactEnv, observabilityEnv)
	setIfNotEmpty(ingestionEnv, "LOG_LEVEL", ingester.Spec.Ingestion.LogLevel)
	setIfNotEmpty(ingestionEnv, "DOCLING_LOG_LEVEL", ingester.Spec.Ingestion.DoclingLogLevel)
	ingestionEnv["PYTHONUNBUFFERED"] = strconv.FormatBool(ingester.Spec.Ingestion.PythonUnbuffered)
	setIfNotEmpty(ingestionEnv, "DOCLING_CACHE_DIR", ingester.Spec.Ingestion.DoclingCacheDirectory)
	setIfNotEmpty(ingestionEnv, "DOCLING_ARTIFACTS_PATH", ingester.Spec.Ingestion.DoclingArtifactsPath)
	ingestionEnv["HF_HUB_ENABLE_HF_TRANSFER"] = boolInt(ingester.Spec.Ingestion.HuggingFaceTransferEnabled)
	ingestionEnv["HF_HUB_OFFLINE"] = boolInt(ingester.Spec.Ingestion.HuggingFaceOffline)
	ingestionEnv["INGESTION_OCR_ENABLED"] = strconv.FormatBool(ingester.Spec.Ingestion.OCREnabled)

	analysisEnv := mergedEnv(artifactEnv, observabilityEnv)
	analysisEnv = mergedEnv(analysisEnv, llmEnv)
	setIfNotEmpty(analysisEnv, "PYTHONPATH", ingester.Spec.LLMAnalysis.PythonPath)
	analysisEnv["FIGURE_INTERPRETATION_ENABLED"] = strconv.FormatBool(ingester.Spec.LLMAnalysis.FigureInterpretationEnabled)
	analysisEnv["FIGURE_INTERPRETATION_MAX_FIGURES"] = strconv.FormatInt(int64(ingester.Spec.LLMAnalysis.FigureInterpretationMaxFigures), 10)

	pythonEnv := func(spec appsv1alpha1.PythonComponentSpec) map[string]string {
		env := mergedEnv(artifactEnv, observabilityEnv)
		setIfNotEmpty(env, "PYTHONPATH", spec.PythonPath)
		return env
	}
	bffEnv := pythonEnv(ingester.Spec.BFF)
	bffEnv["SONATAFLOW_URL"] = strings.TrimRight(workflowURL, "/")
	if ingester.Spec.ArtifactStore != nil {
		setIfNotEmpty(bffEnv, "MINIO_ENDPOINT", ingester.Spec.ArtifactStore.URL)
	}
	uiEnv := map[string]string{}
	setIfNotEmpty(uiEnv, "BFF_HOST", endpointHost(endpoints["bff"]))

	return []sandboxComponent{
		{Name: "ingestion", Port: 8080, Spec: ingester.Spec.Ingestion.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.ingestion:app", "8080"), Env: ingestionEnv, Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "llm-analysis", Port: 8080, Spec: ingester.Spec.LLMAnalysis.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.llm_analysis:app", "8080"), Env: analysisEnv, Providers: []string{artifactProvider, llmProvider}, Registrations: providerRegistrations(artifactRegistration, llmRegistration), NetworkAccess: combinedNetworkAccess(artifactAccess, inferenceAccess)},
		{Name: "assembly", Port: 8080, Spec: ingester.Spec.Assembly.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.assembly_svc:app", "8080"), Env: pythonEnv(ingester.Spec.Assembly), Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "delivery", Port: 8080, Spec: ingester.Spec.Delivery.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.delivery_svc:app", "8080"), Env: pythonEnv(ingester.Spec.Delivery), Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "bff", Port: 8080, Spec: ingester.Spec.BFF.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.bff:app", "8080"), Env: bffEnv, Providers: []string{artifactProvider}, Registrations: providerRegistrations(artifactRegistration), NetworkAccess: artifactAccess},
		{Name: "ui", Port: 8080, Spec: ingester.Spec.UI, Command: []string{"/usr/libexec/s2i/run"}, Env: uiEnv},
	}
}
