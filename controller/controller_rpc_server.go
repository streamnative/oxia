package controller

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/common/container"
)

type ControllerRpcServer struct {
	container    container.GrpcServer
	healthServer *health.Server
}

func NewControllerRpcServer(bindAddress string) (*ControllerRpcServer, error) {
	server := &ControllerRpcServer{
		healthServer: health.NewServer(),
	}

	var err error
	server.container, err = container.Default.StartGrpcServer("controller", bindAddress, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, server.healthServer)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *ControllerRpcServer) Close() error {
	s.healthServer.Shutdown()
	return s.container.Close()
}
