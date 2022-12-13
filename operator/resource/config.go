package resource

var (
	AllPorts     = []NamedPort{PublicPort, InternalPort, MetricsPort}
	PublicPort   = NamedPort{"public", 6650}
	InternalPort = NamedPort{"internal", 8080}
	MetricsPort  = NamedPort{"metrics", 6648}
)

type NamedPort struct {
	Name string
	Port int
}

type Resources struct {
	Cpu, Memory string
}

type ServiceConfig struct {
	Name     string
	Headless bool
	Ports    []NamedPort
}

type DeploymentConfig struct {
	PodConfig
	Replicas uint32
}

type StatefulSetConfig struct {
	PodConfig
	Replicas uint32
	Volume   string
}

type PodConfig struct {
	Name, Image, Command string
	Args                 []string
	Ports                []NamedPort
	Resources            Resources
	VolumeConfig         *VolumeConfig
}

type VolumeConfig struct {
	Name, Path, Volume string
}
