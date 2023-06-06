package server

import (
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	. "github.com/streamnative/oxia/controllers/common"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func MakeServerStatefulSet(cluster *oxiav1alpha1.OxiaCluster) *appsV1.StatefulSet {
	_resourceName := MakeResourceName(Server, cluster.Name)
	command := []string{
		"oxia",
		"server",
		"--log-json",
		"--data-dir=/data/db",
		"--wal-dir=/data/wal",
	}
	if cluster.Spec.PprofEnabled {
		command = append(command, "--profile")
	}
	statefulSet := &appsV1.StatefulSet{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: appsV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(Server, cluster),
		Spec: appsV1.StatefulSetSpec{
			Replicas:    pointer.Int32(int32(cluster.Spec.Server.Replicas)),
			Selector:    &metaV1.LabelSelector{MatchLabels: SelectorLabels(Server, cluster.Name)},
			ServiceName: _resourceName,
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: ObjectMeta(Server, cluster),
				Spec: coreV1.PodSpec{
					ServiceAccountName: _resourceName,
					Containers: []coreV1.Container{{
						Name:    "server",
						Command: command,
						Image:   Image(&cluster.Spec.Image),
						Ports:   Transform(Ports, ContainerPort),
						Resources: coreV1.ResourceRequirements{Limits: coreV1.ResourceList{
							coreV1.ResourceCPU:    cluster.Spec.Server.Cpu,
							coreV1.ResourceMemory: cluster.Spec.Server.Memory,
						}},
						VolumeMounts:   []coreV1.VolumeMount{{Name: "data", MountPath: "/data"}},
						LivenessProbe:  Probe(),
						ReadinessProbe: Probe(),
					}},
				},
			},
			VolumeClaimTemplates: []coreV1.PersistentVolumeClaim{{
				ObjectMeta: metaV1.ObjectMeta{Name: "data"},
				Spec: coreV1.PersistentVolumeClaimSpec{
					AccessModes:      []coreV1.PersistentVolumeAccessMode{coreV1.ReadWriteOnce},
					StorageClassName: cluster.Spec.Server.StorageClassName,
					Resources: coreV1.ResourceRequirements{
						Requests: coreV1.ResourceList{
							coreV1.ResourceStorage: cluster.Spec.Server.Storage,
						},
					},
				},
			}},
		},
	}
	if cluster.Spec.Image.PullPolicy != nil {
		statefulSet.Spec.Template.Spec.Containers[0].ImagePullPolicy = *cluster.Spec.Image.PullPolicy
	}
	return statefulSet
}

func MakeService(cluster *oxiav1alpha1.OxiaCluster) *coreV1.Service {
	service := &coreV1.Service{
		TypeMeta: metaV1.TypeMeta{
			Kind:       "Service",
			APIVersion: coreV1.SchemeGroupVersion.String(),
		},
		ObjectMeta: ObjectMeta(Server, cluster),
		Spec: coreV1.ServiceSpec{
			Selector:                 SelectorLabels(Server, cluster.Name),
			Ports:                    Transform(Ports, ServicePort),
			ClusterIP:                coreV1.ClusterIPNone,
			PublishNotReadyAddresses: true,
		},
	}
	service.Labels["oxia_cluster"] = cluster.Name
	return service
}
