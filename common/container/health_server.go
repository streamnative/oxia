package container

import (
	"context"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthServer implements `service Health`.
type HealthServer struct {
	healthgrpc.UnimplementedHealthServer

	ctx    context.Context
	cancel context.CancelFunc
}

func NewHealthServer() *HealthServer {
	hs := &HealthServer{}
	hs.ctx, hs.cancel = context.WithCancel(context.Background())
	return hs
}

func (s *HealthServer) Check(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (s *HealthServer) Watch(in *healthpb.HealthCheckRequest, stream healthgrpc.Health_WatchServer) error {
	// Send first update and keep the stream open until it's time to shut down
	go func() {
		if err := stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}); err != nil {
			// Fail to send, close the stream
			s.cancel()
			return
		}
	}()

	select {
	case <-stream.Context().Done():
		// Client has closed the stream
		return nil

	case <-s.ctx.Done():
		// Server is closing
		return s.ctx.Err()
	}
}

func (s *HealthServer) Close() error {
	s.cancel()
	return nil
}
