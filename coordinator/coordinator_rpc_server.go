package coordinator

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/server/container"
)

type CoordinatorRpcServer struct {
	container *container.Container
}

func NewCoordinatorRpcServer(port int) (*CoordinatorRpcServer, error) {
	server := &CoordinatorRpcServer{}

	var err error
	server.container, err = container.Start("coordinator", port, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, health.NewServer())
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *CoordinatorRpcServer) Close() error {
	return s.container.Close()
}
