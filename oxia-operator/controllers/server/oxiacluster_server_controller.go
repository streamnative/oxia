package server

import (
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	"github.com/streamnative/oxia/controllers/common"
	appsV1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Register reconcilers
var reconcilers = []common.Reconcile{
	reconcileServiceAccount,
	reconcileStatefulSet,
	reconcileServices,
	reconcileServiceMonitor,
}

func ReconcileaServer(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	// Chain call reconcilers
	for _, fn := range reconcilers {
		err := fn(subCtx, oxia)
		if err != nil {
			subCtx.Log.Error(err, "Failed to reconcile oxia cluster server.", "NamespaceName",
				types.NamespacedName{Name: common.MakeResourceName(common.Server, oxia.Name), Namespace: oxia.Namespace})
			return err
		}
	}
	return nil
}

func reconcileServiceAccount(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Server, oxia.Name), Namespace: oxia.Namespace}
	_serviceAccount := &corev1.ServiceAccount{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _serviceAccount); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := common.MakeServiceAccount(common.Server, oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_serviceAccount, patch) {
		subCtx.Log.Info("Created server account.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_serviceAccount, patch) {
			subCtx.Log.Info("Updated server account.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileStatefulSet(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Server, oxia.Name), Namespace: oxia.Namespace}
	_statefulSet := &appsV1.StatefulSet{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _statefulSet); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := MakeServerStatefulSet(oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_statefulSet, patch) {
		subCtx.Log.Info("Created server stateful set.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_statefulSet, patch) {
			subCtx.Log.Info("Updated server stateful set.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileServices(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Server, oxia.Name), Namespace: oxia.Namespace}
	_service := &corev1.Service{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _service); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := MakeService(oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_service, patch) {
		subCtx.Log.Info("Created server service.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_service, patch) {
			subCtx.Log.Info("Updated server service.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileServiceMonitor(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	if !oxia.Spec.MonitoringEnabled {
		return nil
	}
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Server, oxia.Name), Namespace: oxia.Namespace}
	_serviceMonitor := &monitoringV1.ServiceMonitor{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _serviceMonitor); err != nil {
		if !apiErrors.IsNotFound(err) {
			if meta.IsNoMatchError(err) {
				subCtx.Log.Info("No Service Monitor Kind, Please confirm you have already deployed prometheus operator")
				return nil
			}
			return err
		}
	}
	patch := common.MakeServiceMonitor(common.Server, oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_serviceMonitor, patch) {
		subCtx.Log.Info("Created server service monitor.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_serviceMonitor, patch) {
			subCtx.Log.Info("Updated server service monitor.", "NamespaceName", nsName)
		}
	}
	return nil
}
