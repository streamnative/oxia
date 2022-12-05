package common

import (
	"context"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"io"
	"oxia/proto"
	"sync"
	"time"
)

const DefaultRpcTimeout = 30 * time.Second

type ClientPool interface {
	io.Closer
	GetClientRpc(target string) (proto.OxiaClientClient, error)
	GetControlRpc(target string) (proto.OxiaControlClient, error)
	GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error)
}

type clientPool struct {
	sync.RWMutex
	connections map[string]grpc.ClientConnInterface

	log zerolog.Logger
}

func NewClientPool() ClientPool {
	return &clientPool{
		connections: make(map[string]grpc.ClientConnInterface),
		log: log.With().
			Str("component", "client-pool").
			Logger(),
	}
}

func (cp *clientPool) Close() error {
	for target, cnx := range cp.connections {
		err := cnx.(*grpc.ClientConn).Close()
		if err != nil {
			cp.log.Warn().
				Str("server_address", target).
				Err(err).
				Msg("Failed to close GRPC connection")
		}
	}
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
	cp.RLock()
	cnx, ok := cp.connections[target]
	cp.RUnlock()
	if ok {
		return cnx, nil
	}

	cp.Lock()
	defer cp.Unlock()

	cnx, ok = cp.connections[target]
	if ok {
		return cnx, nil
	}

	cp.log.Info().
		Str("server_address", target).
		Msg("Creating new GRPC connection")

	cnx, err := grpc.Dial(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
		grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
	)
	if err != nil {
		return nil, err
	}

	cp.connections[target] = cnx
	return cnx, nil
}

func GetPeer(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	return p.Addr.String()
}
