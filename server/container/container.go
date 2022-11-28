package container

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"io"
	"net"
)

type Container struct {
	io.Closer
	server *grpc.Server
	port   int
	log    zerolog.Logger
}

func Start(name string, port int, registerFunc func(grpc.ServiceRegistrar)) (*Container, error) {
	c := &Container{
		server: grpc.NewServer(),
	}

	registerFunc(c.server)

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}

	c.port = listener.Addr().(*net.TCPAddr).Port

	c.log = log.With().
		Str("container", name).
		Str("bindAddress", listener.Addr().String()).
		Logger()

	go func() {
		if err := c.server.Serve(listener); err != nil {
			c.log.Fatal().Err(err).Msg("Failed to start serving")
		}
		fmt.Println("XXX")
	}()

	c.log.Info().Msg("Started container")

	return c, nil
}

func (c *Container) Port() int {
	return c.port
}

func (c *Container) Close() error {
	c.server.GracefulStop()
	c.log.Info().Msg("Stopped container")
	return nil
}
