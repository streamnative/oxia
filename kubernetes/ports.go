package kubernetes

var (
	CoordinatorPorts = []NamedPort{InternalPort, MetricsPort}
	ServerPorts      = []NamedPort{PublicPort, InternalPort, MetricsPort}
	PublicPort       = NamedPort{"public", 6648}
	InternalPort     = NamedPort{"internal", 6649}
	MetricsPort      = NamedPort{"metrics", 8080}
)

type NamedPort struct {
	Name string
	Port int
}
