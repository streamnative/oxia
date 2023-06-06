package coordinator

import (
	"fmt"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	. "github.com/streamnative/oxia/controllers/common"
	"gopkg.in/yaml.v2"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func MakeService(cluster *oxiav1alpha1.OxiaCluster) *coreV1.Service {
	service := &coreV1.Service{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "Service",
			APIVersion: coreV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(Coordinator, cluster),
		Spec: coreV1.ServiceSpec{
			Selector: SelectorLabels(Coordinator, cluster.Name),
			Ports:    Transform(Ports, ServicePort),
		},
	}
	service.Labels["oxia_cluster"] = cluster.Name
	return service
}

func MakeCoordinatorConfigMap(cluster *oxiav1alpha1.OxiaCluster) (*coreV1.ConfigMap, error) {
	servers := make([]ServerAddress, cluster.Spec.Server.Replicas)
	for i := 0; i < int(cluster.Spec.Server.Replicas); i++ {
		servers[i] = ServerAddress{
			Public:   ServiceAddress(cluster.Namespace, cluster.Name, i, PublicPort.Port),
			Internal: fmt.Sprintf("%s-%d.%s:%d", cluster.Name, i, cluster.Name, InternalPort.Port),
		}
	}
	config := ClusterConfig{
		Namespaces: []NamespaceConfig{},
		Servers:    servers,
	}

	for _, ns := range cluster.Spec.Namespaces {
		config.Namespaces = append(config.Namespaces,
			NamespaceConfig{
				Name:              ns.Name,
				InitialShardCount: ns.InitialShardCount,
				ReplicationFactor: ns.ReplicationFactor,
			})
	}

	bytes, err := yaml.Marshal(config)
	return &coreV1.ConfigMap{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: coreV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(Coordinator, cluster),
		Data: map[string]string{
			"config.yaml": string(bytes),
		},
	}, err
}

func MakeCoordinatorDeployment(cluster *oxiav1alpha1.OxiaCluster) *appsV1.Deployment {
	_resourceName := MakeResourceName(Coordinator, cluster.Name)
	command := []string{
		"oxia",
		"coordinator",
		"--log-json",
		"--metadata=configmap",
		fmt.Sprintf("--k8s-namespace=%s", cluster.Namespace),
		fmt.Sprintf("--k8s-configmap-name=%s-status", cluster.Name),
	}
	if cluster.Spec.PprofEnabled {
		command = append(command, "--profile")
	}
	deployment := &appsV1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: appsV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(Coordinator, cluster),
		Spec: appsV1.DeploymentSpec{
			Replicas: pointer.Int32(1), // only one coordinator
			Selector: &metaV1.LabelSelector{MatchLabels: SelectorLabels(Coordinator, cluster.Name)},
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: ObjectMeta(Coordinator, cluster),
				Spec: coreV1.PodSpec{
					ServiceAccountName: _resourceName,
					Containers: []coreV1.Container{{
						Name:    "coordinator",
						Command: command,
						Image:   Image(&cluster.Spec.Image),
						Ports:   Transform(Ports, ContainerPort),
						Resources: coreV1.ResourceRequirements{Limits: coreV1.ResourceList{
							coreV1.ResourceCPU:    cluster.Spec.Coordinator.Cpu,
							coreV1.ResourceMemory: cluster.Spec.Coordinator.Memory,
						}},
						VolumeMounts:   []coreV1.VolumeMount{{Name: "conf", MountPath: "/oxia/conf"}},
						LivenessProbe:  Probe(),
						ReadinessProbe: Probe(),
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
	if cluster.Spec.Image.PullPolicy != nil {
		deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy = *cluster.Spec.Image.PullPolicy
	}
	return deployment
}
