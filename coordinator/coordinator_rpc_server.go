package coordinator

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/common/container"
)

type rpcServer struct {
	grpcServer   container.GrpcServer
	healthServer *health.Server
}

func newRpcServer(bindAddress string) (*rpcServer, error) {
	server := &rpcServer{
		healthServer: health.NewServer(),
	}

	var err error
	server.grpcServer, err = container.Default.StartGrpcServer("coordinator", bindAddress, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, server.healthServer)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *rpcServer) Close() error {
	s.healthServer.Shutdown()
	return s.grpcServer.Close()
}
