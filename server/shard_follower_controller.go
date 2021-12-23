package main

import (
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
}

func NewShardFollowerController(shard uint32, leader string) ShardFollowerController {
	log.Info().
		Uint32("shard", shard).
		Str("leader", leader).
		Msg("Start following")
	return &shardFollowerController{
		shard: shard,
		wal:   NewWal(shard),
	}
}

func (s *shardFollowerController) Close() error {
	log.Info().
		Uint32("shard", s.shard).
		Msg("Closing follower controller")

	return s.wal.Close()
}
