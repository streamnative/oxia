package coordinator

import (
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	"github.com/streamnative/oxia/controllers/common"
	appsV1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Register reconcilers
var reconcilers = []common.Reconcile{
	reconcileServiceAccount,
	reconcileRole,
	reconcileRoleBinding,
	reconcileConfigMap,
	reconcileCoordinatorDeployment,
	reconcileService,
	reconcileServiceMonitor,
}

func ReconcileCoordinator(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	// Chain call reconcilers
	for _, fn := range reconcilers {
		err := fn(subCtx, oxia)
		if err != nil {
			return err
		}
	}
	return nil
}

func reconcileServiceAccount(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
	_serviceAccount := &corev1.ServiceAccount{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _serviceAccount); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := common.MakeServiceAccount(common.Coordinator, oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_serviceAccount, patch) {
		subCtx.Log.Info("Created coordinator service account.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_serviceAccount, patch) {
			subCtx.Log.Info("Updated coordinator service account.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileRole(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
	_role := &rbacV1.Role{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _role); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := common.MakeRole(common.Coordinator, oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_role, patch) {
		subCtx.Log.Info("Created coordinator role.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_role, patch) {
			subCtx.Log.Info("Updated coordinator role.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileRoleBinding(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
	_roleBinding := &rbacV1.RoleBinding{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _roleBinding); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := common.MakeRoleBinding(common.Coordinator, oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_roleBinding, patch) {
		subCtx.Log.Info("Created coordinator role binding.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_roleBinding, patch) {
			subCtx.Log.Info("Updated coordinator role binding.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileCoordinatorDeployment(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
	_deployment := &appsV1.Deployment{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _deployment); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch := MakeCoordinatorDeployment(oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_deployment, patch) {
		subCtx.Log.Info("Created coordinator deployment.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_deployment, patch) {
			subCtx.Log.Info("Updated coordinator deployment.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileService(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
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
		subCtx.Log.Info("Created coordinator service.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_service, patch) {
			subCtx.Log.Info("Updated coordinator service.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileConfigMap(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
	_configMap := &corev1.ConfigMap{}
	if err := subCtx.Get(subCtx.InnerCtx, nsName, _configMap); err != nil {
		if !apiErrors.IsNotFound(err) {
			return err
		}
	}
	patch, err := MakeCoordinatorConfigMap(oxia)
	if err != nil {
		return err
	}
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_configMap, patch) {
		subCtx.Log.Info("Created coordinator config map.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_configMap, patch) {
			subCtx.Log.Info("Updated coordinator config map.", "NamespaceName", nsName)
		}
	}
	return nil
}

func reconcileServiceMonitor(subCtx *common.SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error {
	if !oxia.Spec.MonitoringEnabled {
		return nil
	}
	nsName := types.NamespacedName{Name: common.MakeResourceName(common.Coordinator, oxia.Name), Namespace: oxia.Namespace}
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
	patch := common.MakeServiceMonitor(common.Coordinator, oxia)
	if err := subCtx.Patch(subCtx.InnerCtx, patch,
		client.Apply, client.FieldOwner(common.FieldOwner), client.ForceOwnership); err != nil {
		return err
	}
	if common.IsResourceFirstCreated(_serviceMonitor, patch) {
		subCtx.Log.Info("Created coordinator service monitor.", "NamespaceName", nsName)
	} else {
		if common.IsResourceUpdated(_serviceMonitor, patch) {
			subCtx.Log.Info("Updated coordinator service monitor.", "NamespaceName", nsName)
		}
	}
	return nil
}
