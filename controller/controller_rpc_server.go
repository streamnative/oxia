package controller

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/server/container"
)

type ControllerRpcServer struct {
	container *container.Container
}

func NewControllerRpcServer(port int) (*ControllerRpcServer, error) {
	server := &ControllerRpcServer{}

	var err error
	server.container, err = container.Start("controller", port, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, health.NewServer())
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *ControllerRpcServer) Close() error {
	return s.container.Close()
}
