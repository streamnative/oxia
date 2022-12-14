package impl

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"math/rand"
	"oxia/common"
	"oxia/proto"
	"sync"
	"time"
)

// The ShardController is responsible to handle all the state transition for a given a shard
// e.g. electing a new leader
type ShardController interface {
	io.Closer

	HandleNodeFailure(failedNode ServerAddress)

	Epoch() int64
	Leader() *ServerAddress
	Status() ShardStatus
}

type shardController struct {
	sync.Mutex

	shard         uint32
	shardMetadata ShardMetadata
	rpc           RpcProvider
	coordinator   Coordinator

	ctx    context.Context
	cancel context.CancelFunc
	log    zerolog.Logger
}

func NewShardController(shard uint32, shardMetadata ShardMetadata, rpc RpcProvider, coordinator Coordinator) ShardController {
	s := &shardController{
		shard:         shard,
		shardMetadata: shardMetadata,
		rpc:           rpc,
		coordinator:   coordinator,
		log: log.With().
			Str("component", "shard-controller").
			Uint32("shard", shard).
			Logger(),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.log.Info().
		Interface("shard-metadata", s.shardMetadata).
		Msg("Started shard controller")

	s.electLeaderWithRetries()
	return s
}

func (s *shardController) HandleNodeFailure(failedNode ServerAddress) {
	s.Lock()
	defer s.Unlock()

	s.log.Debug().
		Interface("failed-node", failedNode).
		Interface("current-leader", s.shardMetadata.Leader).
		Msg("Received notification of failed node")

	if s.shardMetadata.Leader != nil &&
		*s.shardMetadata.Leader == failedNode {
		s.log.Info().
			Interface("leader", failedNode).
			Msg("Detected failure on shard leader")
		s.electLeaderWithRetries()
	}
}

func (s *shardController) electLeaderWithRetries() {
	go common.DoWithLabels(map[string]string{
		"oxia":  "shard-controller-leader-election",
		"shard": fmt.Sprintf("%d", s.shard),
	}, func() {
		_ = backoff.RetryNotify(s.electLeader, common.NewBackOff(s.ctx),
			func(err error, duration time.Duration) {
				s.log.Warn().Err(err).
					Dur("retry-after", duration).
					Msg("Leader election has failed, retrying later")
			})
	})
}

func (s *shardController) electLeader() error {
	s.Lock()
	defer s.Unlock()

	s.shardMetadata.Status = ShardStatusElection
	s.shardMetadata.Leader = nil
	s.shardMetadata.Epoch++
	s.log.Info().
		Int64("epoch", s.shardMetadata.Epoch).
		Msg("Starting leader election")

	if err := s.coordinator.InitiateLeaderElection(s.shard, s.shardMetadata); err != nil {
		return err
	}

	// Fence all the ensemble members
	fr, err := s.fenceQuorum()
	if err != nil {
		return err
	}

	newLeader, followers := s.selectNewLeader(fr)

	s.log.Info().
		Int64("epoch", s.shardMetadata.Epoch).
		Interface("new-leader", newLeader).
		Interface("followers", followers).
		Msg("Successfully fenced ensemble")

	if err = s.becomeLeader(newLeader, followers); err != nil {
		return err
	}

	metadata := s.shardMetadata.Clone()
	metadata.Status = ShardStatusSteadyState
	metadata.Leader = &newLeader

	if err = s.coordinator.ElectedLeader(s.shard, metadata); err != nil {
		return err
	}

	s.shardMetadata = metadata

	s.log.Info().
		Int64("epoch", s.shardMetadata.Epoch).
		Interface("leader", s.shardMetadata.Leader).
		Msg("Elected new leader")
	return nil
}

func (s *shardController) fenceQuorum() (map[ServerAddress]*proto.EntryId, error) {
	majority := len(s.shardMetadata.Ensemble)/2 + 1

	res := make(map[ServerAddress]*proto.EntryId)

	// TODO: send fence requests in parallel
	successResponses := 0
	for _, sa := range s.shardMetadata.Ensemble {
		entryId, err := s.fence(sa)
		if err != nil {
			s.log.Warn().Err(err).
				Str("node", sa.Internal).
				Msg("Failed to fence node")
			continue
		}

		successResponses++
		res[sa] = entryId
		s.log.Info().
			Interface("server-address", sa).
			Interface("entry-id", entryId).
			Int64("epoch", s.shardMetadata.Epoch).
			Msg("Processed fence response")
	}

	if successResponses < majority {
		return nil, errors.New("failed to fence shard")
	}

	return res, nil
}

func (s *shardController) fence(node ServerAddress) (*proto.EntryId, error) {
	res, err := s.rpc.Fence(s.ctx, node, &proto.FenceRequest{
		ShardId: s.shard,
		Epoch:   s.shardMetadata.Epoch,
	})
	if err != nil {
		return nil, err
	}

	if res.Epoch != s.shardMetadata.Epoch {
		return nil, errors.New("invalid epoch")
	}

	return res.HeadIndex, nil
}

func (s *shardController) selectNewLeader(fenceResponses map[ServerAddress]*proto.EntryId) (
	leader ServerAddress, followers map[string]*proto.EntryId) {
	// Select all the nodes that have the highest entry in the wal
	var currentMax int64 = -1
	var candidates []ServerAddress

	for addr, headIndex := range fenceResponses {
		if headIndex.Offset < currentMax {
			continue
		} else if headIndex.Offset == currentMax {
			candidates = append(candidates, addr)
		} else {
			// Found a new max
			currentMax = headIndex.Offset
			candidates = []ServerAddress{addr}
		}
	}

	// Select a random leader among the nodes with the highest entry in the wal
	leader = candidates[rand.Intn(len(candidates))]
	followers = make(map[string]*proto.EntryId)
	for a, e := range fenceResponses {
		if a != leader {
			followers[a.Internal] = e
		}
	}
	return leader, followers
}

func (s *shardController) becomeLeader(leader ServerAddress, followers map[string]*proto.EntryId) error {
	res, err := s.rpc.BecomeLeader(s.ctx, leader, &proto.BecomeLeaderRequest{
		ShardId:           s.shard,
		Epoch:             s.shardMetadata.Epoch,
		ReplicationFactor: uint32(len(s.shardMetadata.Ensemble)),
		FollowerMaps:      followers,
	})
	if err != nil {
		return err
	}

	if res.Epoch != s.shardMetadata.Epoch {
		return errors.New("invalid epoch")
	}

	return nil
}

func (s *shardController) Epoch() int64 {
	s.Lock()
	defer s.Unlock()
	return s.shardMetadata.Epoch
}

func (s *shardController) Leader() *ServerAddress {
	s.Lock()
	defer s.Unlock()
	return s.shardMetadata.Leader
}

func (s *shardController) Status() ShardStatus {
	s.Lock()
	defer s.Unlock()
	return s.shardMetadata.Status
}

func (s *shardController) Close() error {
	s.cancel()
	return nil
}
