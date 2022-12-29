package container

import (
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"io"
	"net"
	"oxia/common"
)

type GrpcServer interface {
	io.Closer

	Port() int
}

type GrpcProvider interface {
	StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error)
}

var Default = &defaultProvider{}

type defaultProvider struct {
}

func (d *defaultProvider) StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error) {
	return newDefaultGrpcProvider(name, bindAddress, registerFunc)
}

type defaultGrpcServer struct {
	io.Closer
	server *grpc.Server
	port   int
	log    zerolog.Logger
}

func newDefaultGrpcProvider(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error) {
	c := &defaultGrpcServer{
		server: grpc.NewServer(
			grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		),
	}
	registerFunc(c.server)
	grpc_prometheus.Register(c.server)

	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	c.port = listener.Addr().(*net.TCPAddr).Port

	c.log = log.With().
		Str("grpc-server", name).
		Str("bindAddress", listener.Addr().String()).
		Logger()

	go common.DoWithLabels(map[string]string{
		"oxia": name,
		"bind": listener.Addr().String(),
	}, func() {
		if err := c.server.Serve(listener); err != nil {
			c.log.Fatal().Err(err).Msg("Failed to start serving")
		}
	})

	c.log.Info().Msg("Started Grpc server")

	return c, nil
}

func (c *defaultGrpcServer) Port() int {
	return c.port
}

func (c *defaultGrpcServer) Close() error {
	c.server.GracefulStop()
	c.log.Info().Msg("Stopped Grpc server")
	return nil
}
