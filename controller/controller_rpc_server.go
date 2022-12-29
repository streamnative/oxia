package controller

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/common/container"
)

type ControllerRpcServer struct {
	container container.GrpcServer
}

func NewControllerRpcServer(bindAddress string) (*ControllerRpcServer, error) {
	server := &ControllerRpcServer{}

	var err error
	server.container, err = container.Default.StartGrpcServer("controller", bindAddress, func(registrar grpc.ServiceRegistrar) {
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
