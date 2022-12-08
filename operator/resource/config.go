package resource

const (
	PublicPortName   = "public"
	InternalPortName = "internal"
	MetricsPortName  = "metrics"
)

var Ports = map[string]int{
	PublicPortName:   6648,
	InternalPortName: 6649,
	MetricsPortName:  8080,
}

func PublicPort() int {
	return Ports[PublicPortName]
}

func InternalPort() int {
	return Ports[InternalPortName]
}

func MetricsPort() int {
	return Ports[MetricsPortName]
}

type Resources struct {
	Cpu    string
	Memory string
}
