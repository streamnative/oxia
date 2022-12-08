package resource

import (
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

func ServicePort(name string, port int) coreV1.ServicePort {
	return coreV1.ServicePort{
		Name:       name,
		TargetPort: intstr.FromString(name),
		Port:       int32(port),
	}
}

func ContainerPort(name string, port int) coreV1.ContainerPort {
	return coreV1.ContainerPort{
		ContainerPort: int32(port),
		Name:          name,
	}
}

func List(resources Resources) coreV1.ResourceList {
	return coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse(resources.Cpu),
		coreV1.ResourceMemory: resource.MustParse(resources.Memory),
	}
}

func Transform[To any](ports map[string]int, toFunc func(string, int) To) []To {
	to := make([]To, 0)
	for name, port := range ports {
		to = append(to, toFunc(name, port))
	}
	return to
}
