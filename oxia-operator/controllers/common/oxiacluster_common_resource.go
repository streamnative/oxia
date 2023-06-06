package common

import (
	"fmt"
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	"golang.org/x/exp/maps"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func Image(image *oxiav1alpha1.Image) string {
	return fmt.Sprintf("%s:%s", image.Repository, image.Tag)
}

func ServiceAddress(namespace, name string, ordinal, port int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", name, ordinal, name, namespace, port)
}

func Probe() *coreV1.Probe {
	return &coreV1.Probe{
		ProbeHandler: coreV1.ProbeHandler{
			Exec: &coreV1.ExecAction{
				Command: []string{"oxia", "health", fmt.Sprintf("--port=%d", InternalPort.Port)},
			},
			//The GRPC probe can be used instead of Exec once kubernetes <v1.24 is deemed established enough
			//GRPC: &coreV1.GRPCAction{Port: int32(InternalPort.Port)},
		},
		InitialDelaySeconds: 10,
		TimeoutSeconds:      10,
	}
}

func policyRule(apiGroup string, resources []string, verbs []string) rbacV1.PolicyRule {
	return rbacV1.PolicyRule{
		APIGroups: []string{apiGroup},
		Resources: resources,
		Verbs:     verbs,
	}
}

func Transform[To any](ports []NamedPort, toFunc func(NamedPort) To) []To {
	to := make([]To, len(ports))
	for i, port := range ports {
		to[i] = toFunc(port)
	}
	return to
}

func MakeResourceName(component Component, name string) string {
	if component == Coordinator {
		return name + "-coordinator"
	}
	return name
}

func ObjectMeta(component Component, oxia *oxiav1alpha1.OxiaCluster) metaV1.ObjectMeta {
	_resourceName := MakeResourceName(component, oxia.Name)
	return metaV1.ObjectMeta{
		Name:      _resourceName,
		Namespace: oxia.Namespace,
		Labels:    allLabels(component, oxia.Name),
		OwnerReferences: []metaV1.OwnerReference{
			*metaV1.NewControllerRef(oxia, oxia.GroupVersionKind()),
		},
	}
}

func allLabels(component Component, name string) map[string]string {
	_allLabels := make(map[string]string)
	maps.Copy(_allLabels, SelectorLabels(component, name))
	//TODO inject version label
	maps.Copy(_allLabels, additionalLabels("TODO"))
	return _allLabels
}

func SelectorLabels(component Component, name string) map[string]string {
	_resourceName := MakeResourceName(component, name)
	return map[string]string{
		"app.kubernetes.io/name":      "oxia-cluster",
		"app.kubernetes.io/Component": string(component),
		"app.kubernetes.io/instance":  _resourceName,
	}
}
func additionalLabels(version string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/version":    version,
		"app.kubernetes.io/part-of":    "oxia",
		"app.kubernetes.io/managed-by": "oxia-controller",
	}
}

func ContainerPort(port NamedPort) coreV1.ContainerPort {
	return coreV1.ContainerPort{
		ContainerPort: int32(port.Port),
		Name:          port.Name,
	}
}

func MakeServiceAccount(component Component, cluster *oxiav1alpha1.OxiaCluster) *coreV1.ServiceAccount {
	_serviceAccount := &coreV1.ServiceAccount{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       "ServiceAccount",
		},
		ObjectMeta: ObjectMeta(component, cluster),
	}
	if cluster.Spec.Image.PullSecrets != nil {
		_serviceAccount.ImagePullSecrets = []coreV1.LocalObjectReference{{Name: *cluster.Spec.Image.PullSecrets}}
	}
	return _serviceAccount
}

func MakeServiceMonitor(component Component, cluster *oxiav1alpha1.OxiaCluster) *monitoringV1.ServiceMonitor {
	return &monitoringV1.ServiceMonitor{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "ServiceMonitor",
			APIVersion: monitoringV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(component, cluster),
		Spec: monitoringV1.ServiceMonitorSpec{
			Selector:     metaV1.LabelSelector{MatchLabels: SelectorLabels(component, cluster.Name)},
			Endpoints:    []monitoringV1.Endpoint{{Port: MetricsPort.Name}},
			TargetLabels: []string{"oxia_cluster"},
		},
	}
}

func MakeRole(component Component, cluster *oxiav1alpha1.OxiaCluster) *rbacV1.Role {
	if component == Coordinator {
		return &rbacV1.Role{
			TypeMeta: metaV1.TypeMeta{
				Kind:       "Role",
				APIVersion: rbacV1.SchemeGroupVersion.String(),
			},
			ObjectMeta: ObjectMeta(component, cluster),
			Rules: []rbacV1.PolicyRule{
				//If storing shard state on the OxiaCluster status
				policyRule("oxia.streamnative.io", []string{"oxiaclusters"}, []string{"get", "update"}),
				//If storing shard state on a configmap data
				policyRule("", []string{"configmaps"}, []string{"*"}),
			},
		}
	}
	return &rbacV1.Role{
		ObjectMeta: ObjectMeta(component, cluster),
		Rules:      []rbacV1.PolicyRule{},
	}
}

func MakeRoleBinding(component Component, cluster *oxiav1alpha1.OxiaCluster) *rbacV1.RoleBinding {
	_resourceName := MakeResourceName(component, cluster.Name)
	return &rbacV1.RoleBinding{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "RoleBinding",
			APIVersion: rbacV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(component, cluster),
		Subjects: []rbacV1.Subject{{
			Kind:      "ServiceAccount",
			Name:      _resourceName,
			Namespace: cluster.Namespace,
		}},
		RoleRef: rbacV1.RoleRef{
			APIGroup: "",
			Kind:     "Role",
			Name:     _resourceName,
		},
	}
}

func ServicePort(port NamedPort) coreV1.ServicePort {
	return coreV1.ServicePort{
		Name:       port.Name,
		TargetPort: intstr.FromString(port.Name),
		Port:       int32(port.Port),
	}
}

func IsResourceFirstCreated(origin, applied metaV1.Object) bool {
	return origin.GetCreationTimestamp().Time.IsZero() && !applied.GetCreationTimestamp().Time.IsZero()
}

func IsResourceUpdated(origin, applied metaV1.Object) bool {
	if IsResourceFirstCreated(origin, applied) {
		return true
	}
	return origin.GetGeneration() != applied.GetGeneration()
}
