package resource

import (
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Meta(name string) metaV1.ObjectMeta {
	return metaV1.ObjectMeta{
		Name:   name,
		Labels: Labels(name),
	}
}

// TODO add recommended labels https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func Labels(name string) map[string]string {
	return map[string]string{
		"app": name,
	}
}
