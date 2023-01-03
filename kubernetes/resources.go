package kubernetes

import (
	"fmt"
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v2"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	"oxia/coordinator/model"
	"oxia/pkg/apis/oxia/v1alpha1"
)

type Component string

var (
	Coordinator Component = "coordinator"
	Server      Component = "server"
)

func resourceName(component Component, name string) string {
	if component == Coordinator {
		return name + "-coordinator"
	}
	return name
}

func objectMeta(component Component, name string) metaV1.ObjectMeta {
	_resourceName := resourceName(component, name)
	return metaV1.ObjectMeta{
		Name:   _resourceName,
		Labels: allLabels(component, name),
	}
}

func allLabels(component Component, name string) map[string]string {
	_allLabels := make(map[string]string)
	maps.Copy(_allLabels, selectorLabels(component, name))
	//TODO inject version label
	maps.Copy(_allLabels, additionalLabels("TODO"))
	return _allLabels
}

func selectorLabels(component Component, name string) map[string]string {
	_resourceName := resourceName(component, name)
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

func resourceList(resources v1alpha1.Resources) coreV1.ResourceList {
	return coreV1.ResourceList{
		coreV1.ResourceCPU:    k8sResource.MustParse(resources.Cpu),
		coreV1.ResourceMemory: k8sResource.MustParse(resources.Memory),
	}
}

func serviceAccount(component Component, cluster v1alpha1.OxiaCluster) *coreV1.ServiceAccount {
	_serviceAccount := &coreV1.ServiceAccount{
		ObjectMeta: objectMeta(component, cluster.Name),
	}
	if cluster.Spec.ImagePullSecrets != nil {
		_serviceAccount.ImagePullSecrets = []coreV1.LocalObjectReference{{Name: *cluster.Spec.ImagePullSecrets}}
	}
	return _serviceAccount
}

func role(cluster v1alpha1.OxiaCluster) *rbacV1.Role {
	return &rbacV1.Role{
		ObjectMeta: objectMeta(Coordinator, cluster.Name),
		Rules:      policyRules(),
	}
}

func policyRules() []rbacV1.PolicyRule {
	return []rbacV1.PolicyRule{
		//If storing shard state on the OxiaCluster status
		policyRule("oxia.streamnative.io", []string{"oxiaclusters"}, []string{"get", "update"}),
		//If storing shard state on a configmap data
		policyRule("", []string{"configmaps"}, []string{"*"}),
	}
}

func roleBinding(cluster v1alpha1.OxiaCluster) *rbacV1.RoleBinding {
	_resourceName := resourceName(Coordinator, cluster.Name)
	return &rbacV1.RoleBinding{
		ObjectMeta: objectMeta(Coordinator, cluster.Name),
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

func service(component Component, cluster v1alpha1.OxiaCluster, ports []NamedPort) *coreV1.Service {
	var clusterIp string
	if component == Server {
		clusterIp = coreV1.ClusterIPNone
	} else {
		clusterIp = ""
	}
	return &coreV1.Service{
		ObjectMeta: objectMeta(component, cluster.Name),
		Spec: coreV1.ServiceSpec{
			Selector:  selectorLabels(component, cluster.Name),
			Ports:     transform(ports, servicePort),
			ClusterIP: clusterIp,
		},
	}
}

func configMap(cluster v1alpha1.OxiaCluster) *coreV1.ConfigMap {
	servers := make([]model.ServerAddress, *cluster.Spec.ServerReplicas)
	for i := 0; i < int(*cluster.Spec.ServerReplicas); i++ {
		servers[i] = model.ServerAddress{
			Public:   serviceAddress(cluster.Namespace, cluster.Name, i, PublicPort.Port),
			Internal: serviceAddress(cluster.Namespace, cluster.Name, i, InternalPort.Port),
		}
	}
	config := model.ClusterConfig{
		ReplicationFactor: 1,
		ShardCount:        2,
		Servers:           servers,
	}
	bytes, err := yaml.Marshal(config)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to marshal cluster config to yaml")
	}
	return &coreV1.ConfigMap{
		ObjectMeta: objectMeta(Coordinator, cluster.Name),
		Data: map[string]string{
			"config.yaml": string(bytes),
		},
	}
}

func serviceAddress(namespace, name string, ordinal, port int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", name, ordinal, name, namespace, port)
}

func coordinatorDeployment(cluster v1alpha1.OxiaCluster) *appsV1.Deployment {
	_resourceName := resourceName(Coordinator, cluster.Name)
	return &appsV1.Deployment{
		ObjectMeta: objectMeta(Coordinator, cluster.Name),
		Spec: appsV1.DeploymentSpec{
			Replicas: pointer.Int32(1),
			Selector: &metaV1.LabelSelector{MatchLabels: selectorLabels(Coordinator, cluster.Name)},
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: objectMeta(Coordinator, cluster.Name),
				Spec: coreV1.PodSpec{
					ServiceAccountName: _resourceName,
					Containers: []coreV1.Container{{
						Name: "coordinator",
						Command: []string{
							"oxia",
							"coordinator",
							"--log-json",
							"--metadata=configmap",
							fmt.Sprintf("--k8s-namespace=%s", cluster.Namespace),
							fmt.Sprintf("--k8s-configmap-name=%s-status", cluster.Name),
						},
						Image:           cluster.Spec.Image,
						ImagePullPolicy: cluster.Spec.ImagePullPolicy,
						Ports:           transform(CoordinatorPorts, containerPort),
						Resources:       coreV1.ResourceRequirements{Limits: resourceList(cluster.Spec.CoordinatorResources)},
						VolumeMounts:    []coreV1.VolumeMount{{Name: "conf", MountPath: "/oxia/conf"}},
						LivenessProbe:   probe(),
						ReadinessProbe:  probe(),
					}},
					Volumes: []coreV1.Volume{{
						Name: "conf",
						VolumeSource: coreV1.VolumeSource{
							ConfigMap: &coreV1.ConfigMapVolumeSource{
								LocalObjectReference: coreV1.LocalObjectReference{Name: _resourceName},
							},
						},
					}},
				},
			},
		},
	}
}

func serverStatefulSet(cluster v1alpha1.OxiaCluster) *appsV1.StatefulSet {
	_resourceName := resourceName(Server, cluster.Name)
	return &appsV1.StatefulSet{
		ObjectMeta: objectMeta(Server, cluster.Name),
		Spec: appsV1.StatefulSetSpec{
			Replicas:    pointer.Int32(int32(*cluster.Spec.ServerReplicas)),
			Selector:    &metaV1.LabelSelector{MatchLabels: selectorLabels(Server, cluster.Name)},
			ServiceName: _resourceName,
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: objectMeta(Server, cluster.Name),
				Spec: coreV1.PodSpec{
					ServiceAccountName: _resourceName,
					Containers: []coreV1.Container{{
						Name:            "server",
						Command:         []string{"oxia", "server", "--log-json", "--data-dir=/data/db", "--wal-dir=/data/wal"},
						Image:           cluster.Spec.Image,
						ImagePullPolicy: cluster.Spec.ImagePullPolicy,
						Ports:           transform(ServerPorts, containerPort),
						Resources:       coreV1.ResourceRequirements{Limits: resourceList(cluster.Spec.ServerResources)},
						VolumeMounts:    []coreV1.VolumeMount{{Name: "data", MountPath: "/data"}},
						LivenessProbe:   probe(),
						ReadinessProbe:  probe(),
					}},
				},
			},
			VolumeClaimTemplates: []coreV1.PersistentVolumeClaim{{
				ObjectMeta: metaV1.ObjectMeta{Name: "data"},
				Spec: coreV1.PersistentVolumeClaimSpec{
					AccessModes:      []coreV1.PersistentVolumeAccessMode{coreV1.ReadWriteOnce},
					StorageClassName: cluster.Spec.StorageClassName,
					Resources: coreV1.ResourceRequirements{
						Requests: coreV1.ResourceList{
							coreV1.ResourceStorage: k8sResource.MustParse(cluster.Spec.ServerVolume),
						},
					},
				},
			}},
		},
	}
}

func probe() *coreV1.Probe {
	return &coreV1.Probe{
		ProbeHandler:        coreV1.ProbeHandler{GRPC: &coreV1.GRPCAction{Port: int32(InternalPort.Port)}},
		InitialDelaySeconds: 10,
		TimeoutSeconds:      10,
	}
}

func serviceMonitor(component Component, cluster v1alpha1.OxiaCluster) *monitoringV1.ServiceMonitor {
	return &monitoringV1.ServiceMonitor{
		ObjectMeta: objectMeta(component, cluster.Name),
		Spec: monitoringV1.ServiceMonitorSpec{
			Selector:  metaV1.LabelSelector{MatchLabels: selectorLabels(component, cluster.Name)},
			Endpoints: []monitoringV1.Endpoint{{Port: MetricsPort.Name}},
		},
	}
}

func policyRule(apiGroup string, resources []string, verbs []string) rbacV1.PolicyRule {
	return rbacV1.PolicyRule{
		APIGroups: []string{apiGroup},
		Resources: resources,
		Verbs:     verbs,
	}
}

func servicePort(port NamedPort) coreV1.ServicePort {
	return coreV1.ServicePort{
		Name:       port.Name,
		TargetPort: intstr.FromString(port.Name),
		Port:       int32(port.Port),
	}
}

func containerPort(port NamedPort) coreV1.ContainerPort {
	return coreV1.ContainerPort{
		ContainerPort: int32(port.Port),
		Name:          port.Name,
	}
}

func transform[To any](ports []NamedPort, toFunc func(NamedPort) To) []To {
	to := make([]To, len(ports))
	for i, port := range ports {
		to[i] = toFunc(port)
	}
	return to
}
