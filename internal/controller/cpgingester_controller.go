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

// CPGIngesterReconciler reconciles a CPGIngester object
type CPGIngesterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=cpgingesters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=cpgingesters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=cpgingesters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=sandboxrequests,verbs=get;list;watch;create;update;patch;delete

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

	summary, err := reconcileComponentSandboxes(
		ctx,
		r.Client,
		r.Scheme,
		&ingester,
		ingester.Spec.Sandbox,
		cpgIngesterComponents(&ingester),
	)
	if err != nil {
		return ctrl.Result{}, err
	}

	before := ingester.DeepCopy()
	ingester.Status.ObservedGeneration = ingester.Generation
	meta.SetStatusCondition(&ingester.Status.Conditions, metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ingester.Generation,
		Reason:             "SpecAccepted",
		Message:            "The CPGIngester component sandboxes have been reconciled",
	})
	setSandboxReadyCondition(&ingester.Status.Conditions, ingester.Generation, summary)
	if err := r.Status().Patch(ctx, &ingester, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("reconciled CPGIngester sandboxes", "generation", ingester.Generation, "ready", summary.Ready, "desired", summary.Desired)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CPGIngesterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.CPGIngester{}).
		Owns(&appsv1alpha1.SandboxRequest{}).
		Named("cpgingester").
		Complete(r)
}

func cpgIngesterComponents(ingester *appsv1alpha1.CPGIngester) []sandboxComponent {
	artifactEnv, artifactProvider := artifactStoreInputs(ingester.Spec.ArtifactStore)
	observabilityEnv := observabilityInputs(ingester.Spec.Observability)
	llmEnv, llmProvider := llmInputs(ingester.Spec.LLM)

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
	if ingester.Spec.ArtifactStore != nil {
		setIfNotEmpty(bffEnv, "MINIO_ENDPOINT", ingester.Spec.ArtifactStore.URL)
	}

	return []sandboxComponent{
		{Name: "ingestion", Port: 8080, Spec: ingester.Spec.Ingestion.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.ingestion:app", "8080"), Env: ingestionEnv, Providers: []string{artifactProvider}},
		{Name: "llm-analysis", Port: 8080, Spec: ingester.Spec.LLMAnalysis.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.llm_analysis:app", "8080"), Env: analysisEnv, Providers: []string{artifactProvider, llmProvider}},
		{Name: "assembly", Port: 8080, Spec: ingester.Spec.Assembly.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.assembly_svc:app", "8080"), Env: pythonEnv(ingester.Spec.Assembly), Providers: []string{artifactProvider}},
		{Name: "delivery", Port: 8080, Spec: ingester.Spec.Delivery.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.delivery_svc:app", "8080"), Env: pythonEnv(ingester.Spec.Delivery), Providers: []string{artifactProvider}},
		{Name: "bff", Port: 8080, Spec: ingester.Spec.BFF.ComponentSpec, Command: pythonServiceCommand("cpg_ingester.services.bff:app", "8080"), Env: bffEnv, Providers: []string{artifactProvider}},
		{Name: "ui", Port: 8080, Spec: ingester.Spec.UI, Command: []string{"/usr/libexec/s2i/run"}},
	}
}
