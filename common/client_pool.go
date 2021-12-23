package common

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"io"
	"oxia/proto"
	"sync"
)

type ClientPool interface {
	io.Closer
	GetClientRpc(target string) (proto.ClientAPIClient, error)
	GetInternalRpc(target string) (proto.InternalAPIClient, error)
}

type clientPool struct {
	connections sync.Map

	log zerolog.Logger
}

func NewClientPool() ClientPool {
	return &clientPool{
		log: log.With().
			Str("component", "client-pool").
			Logger(),
	}
}

func (cp *clientPool) Close() error {
	cp.connections.Range(func(key interface{}, value interface{}) bool {
		err := value.(*grpc.ClientConn).Close()
		if err != nil {
			cp.log.Warn().
				Str("server_address", key.(string)).
				Err(err).
				Msg("Failed to close GRPC connection")
			return false
		}
		return true
	})

	return nil
}

func (cp *clientPool) GetClientRpc(target string) (proto.ClientAPIClient, error) {
	cnx, err := cp.getConnection(target)
	if err != nil {
		return nil, err
	} else {
		return proto.NewClientAPIClient(cnx), nil
	}
}

func (cp *clientPool) GetInternalRpc(target string) (proto.InternalAPIClient, error) {
	cnx, err := cp.getConnection(target)
	if err != nil {
		return nil, err
	} else {
		return proto.NewInternalAPIClient(cnx), nil
	}
}

func (cp *clientPool) getConnection(target string) (grpc.ClientConnInterface, error) {
	cnx, ok := cp.connections.Load(target)
	if ok {
		return cnx.(grpc.ClientConnInterface), nil
	}

	cp.log.Info().
		Str("server_address", target).
		Msg("Creating new GRPC connection")

	cnx, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		existing, loaded := cp.connections.LoadOrStore(target, cnx)
		if loaded {
			// There was a race condition
			cnx.(*grpc.ClientConn).Close()
			cnx = existing
		}
	}

	return cnx.(grpc.ClientConnInterface), err
}
