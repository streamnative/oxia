package common

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"oxia/proto"
	"sync"
)

type ClientPool interface {
	io.Closer
	GetClientRpc(target string) (proto.OxiaClientClient, error)
	GetControlRpc(target string) (proto.OxiaControlClient, error)
	GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error)
}

type clientPool struct {
	sync.Mutex
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

func (cp *clientPool) GetClientRpc(target string) (proto.OxiaClientClient, error) {
	cnx, err := cp.getConnection(target)
	if err != nil {
		return nil, err
	} else {
		return proto.NewOxiaClientClient(cnx), nil
	}
}

func (cp *clientPool) GetControlRpc(target string) (proto.OxiaControlClient, error) {
	cnx, err := cp.getConnection(target)
	if err != nil {
		return nil, err
	} else {
		return proto.NewOxiaControlClient(cnx), nil
	}
}

func (cp *clientPool) GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error) {
	cnx, err := cp.getConnection(target)
	if err != nil {
		return nil, err
	} else {
		return proto.NewOxiaLogReplicationClient(cnx), nil
	}
}

func (cp *clientPool) getConnection(target string) (grpc.ClientConnInterface, error) {
	cnx, ok := cp.connections.Load(target)
	if ok {
		return cnx.(grpc.ClientConnInterface), nil
	}

	cp.Lock()
	defer cp.Unlock()

	cnx, ok = cp.connections.Load(target)
	if ok {
		return cnx.(grpc.ClientConnInterface), nil
	}

	cp.log.Info().
		Str("server_address", target).
		Msg("Creating new GRPC connection")

	cnx, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	cp.connections.Store(target, cnx)
	return cnx.(grpc.ClientConnInterface), nil
}
