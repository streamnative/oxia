package coordinator

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/common/container"
)

type CoordinatorRpcServer struct {
	grpcServer container.GrpcServer
}

func NewCoordinatorRpcServer(bindAddress string) (*CoordinatorRpcServer, error) {
	server := &CoordinatorRpcServer{}

	var err error
	server.grpcServer, err = container.Default.StartGrpcServer("coordinator", bindAddress, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, health.NewServer())
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *CoordinatorRpcServer) Close() error {
	return s.grpcServer.Close()
}
