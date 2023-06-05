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
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var ServerPorts = []NamedPort{PublicPort, InternalPort, MetricsPort}

// Reconciler

func (r *OxiaClusterReconciler) reconcileServer(ctx context.Context, oxia *oxiav1alpha1.OxiaCluster) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}
