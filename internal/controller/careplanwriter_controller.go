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

// CarePlanWriterReconciler reconciles a CarePlanWriter object
type CarePlanWriterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=careplanwriters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=careplanwriters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.cpgtoacp.io,resources=careplanwriters/finalizers,verbs=update

// Reconcile records that the CarePlanWriter specification has been observed.
// Workload reconciliation will be added as the component deployment contract evolves.
func (r *CarePlanWriterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var writer appsv1alpha1.CarePlanWriter
	if err := r.Get(ctx, req.NamespacedName, &writer); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if writer.Status.ObservedGeneration == writer.Generation &&
		meta.IsStatusConditionTrue(writer.Status.Conditions, "Accepted") {
		return ctrl.Result{}, nil
	}

	before := writer.DeepCopy()
	writer.Status.ObservedGeneration = writer.Generation
	meta.SetStatusCondition(&writer.Status.Conditions, metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: writer.Generation,
		Reason:             "SpecAccepted",
		Message:            "The CarePlanWriter specification has been accepted for reconciliation",
	})
	if err := r.Status().Patch(ctx, &writer, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("accepted CarePlanWriter specification", "generation", writer.Generation)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CarePlanWriterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.CarePlanWriter{}).
		Named("careplanwriter").
		Complete(r)
}
