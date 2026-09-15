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

// Reconcile records that the CPGIngester specification has been observed.
// Workload reconciliation will be added as the component deployment contract evolves.
func (r *CPGIngesterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var ingester appsv1alpha1.CPGIngester
	if err := r.Get(ctx, req.NamespacedName, &ingester); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if ingester.Status.ObservedGeneration == ingester.Generation &&
		meta.IsStatusConditionTrue(ingester.Status.Conditions, "Accepted") {
		return ctrl.Result{}, nil
	}

	before := ingester.DeepCopy()
	ingester.Status.ObservedGeneration = ingester.Generation
	meta.SetStatusCondition(&ingester.Status.Conditions, metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ingester.Generation,
		Reason:             "SpecAccepted",
		Message:            "The CPGIngester specification has been accepted for reconciliation",
	})
	if err := r.Status().Patch(ctx, &ingester, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("accepted CPGIngester specification", "generation", ingester.Generation)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CPGIngesterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.CPGIngester{}).
		Named("cpgingester").
		Complete(r)
}
