package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/proto"
)

type ShardLeaderController interface {
	io.Closer

	Follow(follower string, firstEntry uint64, epoch uint64, ifw proto.InternalAPI_FollowServer) error
}

type shardLeaderController struct {
	shard             uint32
	epoch             uint64
	replicationFactor uint32

	wal Wal
}

func NewShardLeaderController(shard uint32, replicationFactor uint32) ShardLeaderController {
	log.Info().
		Uint32("shard", shard).
		Uint32("replicationFactor", replicationFactor).
		Msg("Start leading")

	return &shardLeaderController{
		shard:             shard,
		replicationFactor: replicationFactor,
		wal:               NewWal(shard),
	}
}

func (s *shardLeaderController) Close() error {
	log.Info().
		Uint32("shard", s.shard).
		Msg("Closing leader controller")

	return s.wal.Close()
}

func (s *shardLeaderController) readLog(firstEntry uint64, ifw proto.InternalAPI_FollowServer) {
	current := firstEntry
	for {
		logEntry, err := s.wal.Read(current)
		if err != nil {
			log.Error().
				Err(err).
				Uint32("shard", s.shard).
				Uint64("entry", current).
				Msg("Failed to read from wal")
			return
		}

		err = ifw.Send(logEntry)
		if err != nil {
			log.Error().
				Err(err).
				Uint32("shard", s.shard).
				Uint64("entry", current).
				Msg("Failed to send entry to follower")
			return
		}
	}
}

func (s *shardLeaderController) Follow(follower string, firstEntry uint64, epoch uint64, ifw proto.InternalAPI_FollowServer) error {
	if epoch != s.epoch {
		return errors.New(fmt.Sprintf("Invalid epoch. Expected: %d - Received: %d", s.epoch, epoch))
	}

	log.Info().
		Uint32("shard", s.shard).
		Uint64("epoch", s.epoch).
		Uint64("firstEntry", firstEntry).
		Str("follower", follower).
		Msg("Follow")

	go s.readLog(firstEntry, ifw)

	for {
		confirmedEntryRequest, err := ifw.Recv()
		if err != nil {
			return err
		}

		log.Info().
			Uint32("shard", s.shard).
			Uint64("epoch", s.epoch).
			Str("confirmedEntryRequest", confirmedEntryRequest.String()).
			Msg("Received confirmed entry request")
	}

	return nil
}
