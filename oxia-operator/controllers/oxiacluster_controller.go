// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"

	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OxiaClusterReconciler reconciles a OxiaCluster object
type OxiaClusterReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

type reconciler func(ctx context.Context, oxia *oxiav1alpha1.OxiaCluster) (ctrl.Result, error)

//+kubebuilder:rbac:groups=oxia.io.streamnative,resources=oxiaclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=oxia.io.streamnative,resources=oxiaclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=oxia.io.streamnative,resources=oxiaclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the OxiaCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *OxiaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("Reconciling OxiaCluster", "Request.Namespace", req.Namespace, "Request.Name", req.Name)
	oxiaCluster := &oxiav1alpha1.OxiaCluster{}
	err := r.Get(ctx, req.NamespacedName, oxiaCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("OxiaCluster ", req.NamespacedName, " not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		r.Log.Error(err, "Failed to get OxiaCluster resource: ", req.NamespacedName)
		return ctrl.Result{}, err
	}

	// Register reconcilers
	reconcilers := []reconciler{
		r.reconcileCoordinator,
		r.reconcileServer,
	}

	// Chain call reconcilers
	for _, r := range reconcilers {
		result, err := r(ctx, oxiaCluster)
		if result.Requeue {
			return result, err
		}
		break
	}

	if err != nil {
		r.Log.Error(err, "Failed to reconcile OxiaCluster resource: ", req.NamespacedName)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *OxiaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&oxiav1alpha1.OxiaCluster{}).
		Complete(r)
}
