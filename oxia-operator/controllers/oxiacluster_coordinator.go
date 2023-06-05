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
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

type ServerAddress struct {
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`
	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

type ClusterConfig struct {
	Namespaces []NamespaceConfig `json:"namespaces" yaml:"namespaces"`
	Servers    []ServerAddress   `json:"servers" yaml:"servers"`
}

type NamespaceConfig struct {
	Name              string `json:"name" yaml:"name"`
	InitialShardCount uint32 `json:"initialShardCount" yaml:"initialShardCount"`
	ReplicationFactor uint32 `json:"replicationFactor" yaml:"replicationFactor"`
}

var CoordinatorPorts = []NamedPort{InternalPort, MetricsPort}

func (r *OxiaClusterReconciler) reconcileCoordinator(ctx context.Context, oxia *oxiav1alpha1.OxiaCluster) (ctrl.Result, error) {
	// Reconcile service account
	_serviceAccount := &coreV1.ServiceAccount{}
	nsName := types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
	err := r.Client.Get(ctx, nsName, _serviceAccount)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to get service account.")
			return ctrl.Result{}, err
		}
		r.Log.Info("Creating a new service account.", "NamespaceName", nsName)
		err := r.Client.Create(ctx, serviceAccount(Coordinator, oxia))
		if err != nil {
			r.Log.Error(err, "Failed to create service account :", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Created service account: ", "NamespaceName", nsName)
		return ctrl.Result{Requeue: true}, nil
	}

	if oxia.DeletionTimestamp != nil {
		r.Log.Info("Service account deletion is unsupported yet!")
	}

	// Reconcile role
	_role := &rbacV1.Role{}
	nsName = types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
	err = r.Client.Get(ctx, nsName, _role)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to get role: ", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Creating a new role :", "NamespaceName", nsName)
		err := r.Client.Create(ctx, role(Coordinator, oxia))
		if err != nil {
			r.Log.Error(err, "Failed to create role:", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Created role: ", "NamespaceName", nsName)
		return ctrl.Result{Requeue: true}, nil
	}

	if oxia.DeletionTimestamp != nil {
		r.Log.Info("Role deletion is unsupported yet!")
	}

	// Reconcile role binding
	_roleBinding := &rbacV1.RoleBinding{}
	nsName = types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
	err = r.Client.Get(ctx, nsName, _roleBinding)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to get role binding: ", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Creating a new role binding:", "NamespaceName", nsName)
		err := r.Client.Create(ctx, roleBinding(Coordinator, oxia))
		if err != nil {
			r.Log.Error(err, "Failed to create role binding:", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Created role binding: ", "NamespaceName", nsName)
		return ctrl.Result{Requeue: true}, nil
	}

	if oxia.DeletionTimestamp != nil {
		r.Log.Info("Role binding deletion is unsupported yet!")
	}

	// reconcile configmap
	_configMap := &coreV1.ConfigMap{}
	nsName = types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
	err = r.Client.Get(ctx, nsName, _configMap)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to get configmap: ", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Creating a new configmap:", "NamespaceName", nsName)
		configMap, err := coordinatorConfigMap(oxia)
		if err != nil {
			r.Log.Error(err, "Failed to create configmap:", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		err = r.Client.Create(ctx, configMap)
		if err != nil {
			r.Log.Error(err, "Failed to create configmap:", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Created configmap: ", "NamespaceName", nsName)
		return ctrl.Result{Requeue: true}, nil
	}

	if oxia.DeletionTimestamp != nil {
		r.Log.Info("Configmap deletion is unsupported yet!")
	}

	// reconcile deployment
	_deployment := &appsV1.Deployment{}
	nsName = types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
	err = r.Client.Get(ctx, nsName, _deployment)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to get deployment: ", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Creating a new deployment:", "NamespaceName", nsName)
		err = r.Client.Create(ctx, coordinatorDeployment(oxia))
		if err != nil {
			r.Log.Error(err, "Failed to create deployment:", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Created deployment: ", "NamespaceName", nsName)
		return ctrl.Result{Requeue: true}, nil
	}

	if oxia.DeletionTimestamp != nil {
		r.Log.Info("Deployment deletion is unsupported yet!")
	}

	// reconcile service
	_service := &coreV1.Service{}
	nsName = types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
	err = r.Client.Get(ctx, nsName, _service)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to get service: ", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Creating a new service:", "NamespaceName", nsName)
		err = r.Client.Create(ctx, service(Coordinator, oxia, CoordinatorPorts))
		if err != nil {
			r.Log.Error(err, "Failed to create service:", "NamespaceName", nsName)
			return ctrl.Result{}, err
		}
		r.Log.Info("Created service: ", "NamespaceName", nsName)
		return ctrl.Result{Requeue: true}, nil
	}

	if oxia.DeletionTimestamp != nil {
		r.Log.Info("Service deletion is unsupported yet!")
	}
	if oxia.Spec.MonitoringEnabled {
		// reconcile service monitor
		_serviceMonitor := &monitoringV1.ServiceMonitor{}
		nsName = types.NamespacedName{Name: resourceName(Coordinator, oxia.Name), Namespace: oxia.Namespace}
		err = r.Client.Get(ctx, nsName, _serviceMonitor)
		if err != nil {
			if meta.IsNoMatchError(err) {
				r.Log.Info("No Service Monitor Kind, Please confirm you have already deployed prometheus operator")
				return ctrl.Result{}, nil
			}
			if !apiErrors.IsNotFound(err) {
				r.Log.Error(err, "Failed to get service monitor: ", "NamespaceName", nsName)
				return ctrl.Result{}, err
			}
			r.Log.Info("Creating a new service monitor:", "NamespaceName", nsName)
			err = r.Client.Create(ctx, serviceMonitor(Coordinator, oxia))
			if err != nil {
				r.Log.Error(err, "Failed to create service monitor:", "NamespaceName", nsName)
				return ctrl.Result{}, err
			}
			r.Log.Info("Created service monitor: ", "NamespaceName", nsName)
			return ctrl.Result{Requeue: true}, nil
		}
	}
	return ctrl.Result{}, nil
}
