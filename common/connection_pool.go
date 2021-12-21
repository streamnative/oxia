package common

import (
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"io"
	"sync"
)

type ConnectionPool interface {
	io.Closer
	GetConnection(target string) (*grpc.ClientConn, error)
}

type connectionPool struct {
	connections sync.Map
}

func NewConnectionPool() ConnectionPool {
	return &connectionPool{}
}

func (c *connectionPool) Close() error {
	c.connections.Range(func(key interface{}, value interface{}) bool {
		err := value.(*grpc.ClientConn).Close()
		if err != nil {
			log.Warn().
				Str("server_address", key.(string)).
				Err(err).
				Msg("Failed to close GRPC connection")
			return false
		}
		return true
	})

	return nil
}

func (c *connectionPool) GetConnection(target string) (*grpc.ClientConn, error) {
	cnx, ok := c.connections.Load(target)
	if ok {
		return cnx.(*grpc.ClientConn), nil
	}

	log.Info().
		Str("server_address", target).
		Msg("Creating new GRPC connection")

	cnx, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		existing, loaded := c.connections.LoadOrStore(target, cnx)
		if loaded {
			// There was a race condition
			cnx.(*grpc.ClientConn).Close()
			cnx = existing
		}
	}

	return cnx.(*grpc.ClientConn), err
}
