package crd

const (
	ClusterScope    = "cluster"
	NamespacedScope = "namespaced"
)

type Config struct {
	Scope     string
	Namespace string
}
