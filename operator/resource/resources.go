package resource

import (
	"fmt"
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"go.uber.org/multierr"
	"io"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

func Meta(name string) metaV1.ObjectMeta {
	return metaV1.ObjectMeta{
		Name:   name,
		Labels: Labels(name),
	}
}

// TODO add recommended labels
// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func Labels(name string) map[string]string {
	return map[string]string{
		"app": name,
	}
}

func ServicePort(port NamedPort) coreV1.ServicePort {
	return coreV1.ServicePort{
		Name:       port.Name,
		TargetPort: intstr.FromString(port.Name),
		Port:       int32(port.Port),
	}
}

func ContainerPort(port NamedPort) coreV1.ContainerPort {
	return coreV1.ContainerPort{
		ContainerPort: int32(port.Port),
		Name:          port.Name,
	}
}

func List(resources Resources) coreV1.ResourceList {
	return coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse(resources.Cpu),
		coreV1.ResourceMemory: resource.MustParse(resources.Memory),
	}
}

func ServiceAccount(name string) *coreV1.ServiceAccount {
	return &coreV1.ServiceAccount{
		ObjectMeta: Meta(name),
	}
}

func Service(config ServiceConfig) *coreV1.Service {
	var clusterIp string
	if config.Headless {
		clusterIp = coreV1.ClusterIPNone
	} else {
		clusterIp = ""
	}
	return &coreV1.Service{
		ObjectMeta: Meta(config.Name),
		Spec: coreV1.ServiceSpec{
			Selector:  Labels(config.Name),
			Ports:     Transform(config.Ports, ServicePort),
			ClusterIP: clusterIp,
		},
	}
}

func Deployment(config DeploymentConfig) *appsV1.Deployment {
	return &appsV1.Deployment{
		ObjectMeta: Meta(config.Name),
		Spec: appsV1.DeploymentSpec{
			Replicas: pointer.Int32(config.Replicas),
			Selector: &metaV1.LabelSelector{MatchLabels: Labels(config.Name)},
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: Meta(config.Name),
				Spec: coreV1.PodSpec{
					ServiceAccountName: config.Name,
					Containers: []coreV1.Container{{
						Name:            config.Name,
						Command:         []string{"oxia", config.Command},
						Image:           config.Image,
						ImagePullPolicy: coreV1.PullIfNotPresent,
						Ports:           Transform(config.Ports, ContainerPort),
						Resources: coreV1.ResourceRequirements{
							Limits: List(config.Resources),
						},
						LivenessProbe:  Probe(),
						ReadinessProbe: Probe(),
					}},
				},
			},
		},
	}
}

func Probe() *coreV1.Probe {
	return &coreV1.Probe{
		ProbeHandler: coreV1.ProbeHandler{
			GRPC: &coreV1.GRPCAction{
				Port: int32(InternalPort.Port),
			},
		},
	}
}

func ServiceMonitor(name string) *monitoringV1.ServiceMonitor {
	return &monitoringV1.ServiceMonitor{
		ObjectMeta: Meta(name),
		Spec: monitoringV1.ServiceMonitorSpec{
			Selector:  metaV1.LabelSelector{MatchLabels: Labels(name)},
			Endpoints: []monitoringV1.Endpoint{{Port: MetricsPort.Name}},
		},
	}
}

func PolicyRule(apiGroup string, resources []string, verbs []string) rbacV1.PolicyRule {
	return rbacV1.PolicyRule{
		APIGroups: []string{apiGroup},
		Resources: resources,
		Verbs:     verbs,
	}
}

func PrintAndAppend(out io.Writer, errs error, err error, operation string, resource string) error {
	if err == nil {
		_, _ = fmt.Fprintf(out, "%s %s succeeded\n", resource, operation)
		return nil
	} else {
		_, _ = fmt.Fprintf(out, "%s %s failed\n", resource, operation)
		return multierr.Append(errs, err)
	}
}

func Transform[To any](ports []NamedPort, toFunc func(NamedPort) To) []To {
	to := make([]To, len(ports))
	for i, port := range ports {
		to[i] = toFunc(port)
	}
	return to
}
