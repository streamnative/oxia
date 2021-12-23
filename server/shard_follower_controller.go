package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
)

type ShardFollowerController interface {
	io.Closer

	//Follow(follower string, firstEntry uint64, epoch uint64, ifw proto.InternalAPI_FollowServer) error
}

type shardFollowerController struct {
	shard uint32
	epoch uint64

	wal Wal
	log zerolog.Logger
}

func NewShardFollowerController(shard uint32, leader string) ShardFollowerController {

	sfc := &shardFollowerController{
		shard: shard,
		wal:   NewWal(shard),
		log: log.With().
			Str("component", "shard-follower").
			Uint32("shard", shard).
			Str("leader", leader).
			Logger(),
	}

	sfc.log.Info().Msg("Start following")
	return sfc
}

func (s *shardFollowerController) Close() error {
	s.log.Info().Msg("Closing follower controller")
	return s.wal.Close()
}
