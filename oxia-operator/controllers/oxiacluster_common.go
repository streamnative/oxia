package controllers

type NamedPort struct {
	Name string
	Port int
}

var (
	PublicPort   = NamedPort{"public", 6648}
	InternalPort = NamedPort{"internal", 6649}
	MetricsPort  = NamedPort{"metrics", 8080}
)

type Component string

var (
	Coordinator Component = "coordinator"
	Server      Component = "server"
)
