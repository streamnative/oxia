package controllers

import (
	"context"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var ServerPorts = []NamedPort{PublicPort, InternalPort, MetricsPort}

// Reconciler

func (r *OxiaClusterReconciler) reconcileServer(ctx context.Context, oxia *oxiav1alpha1.OxiaCluster) (ctrl.Result, error) {

	return ctrl.Result{}, nil
}
