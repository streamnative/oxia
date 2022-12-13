package impl

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"google.golang.org/grpc/status"
	"io"
	"math/rand"
	"oxia/common"
	"oxia/proto"
	"oxia/server"
	"sync"
	"time"
)

const (
	// When fencing quorum of servers, after we reach the majority, wait a bit more
	// to include responses from all healthy servers
	quorumFencingGracePeriod = 100 * time.Millisecond
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

	currentElectionCtx    context.Context
	currentElectionCancel context.CancelFunc
	log                   zerolog.Logger
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

	if s.currentElectionCancel != nil {
		// Cancel any pending activity from the previous election
		s.currentElectionCancel()
	}

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

	s.keepFencingFailedFollowers(followers)
	return nil
}

func (s *shardController) keepFencingFailedFollowers(successfulFollowers map[ServerAddress]*proto.EntryId) {
	if len(successfulFollowers) == len(s.shardMetadata.Ensemble)-1 {
		s.log.Debug().
			Int64("epoch", s.shardMetadata.Epoch).
			Msg("All the member of the ensemble were successfully added")
		return
	}

	s.currentElectionCtx, s.currentElectionCancel = context.WithCancel(s.ctx)

	// Identify failed followers
	for _, sa := range s.shardMetadata.Ensemble {
		if sa == *s.shardMetadata.Leader {
			continue
		}

		if _, found := successfulFollowers[sa]; found {
			continue
		}

		s.keepFencingFollower(s.currentElectionCtx, sa)
	}
}

func (s *shardController) keepFencingFollower(ctx context.Context, node ServerAddress) {
	s.log.Info().
		Interface("follower", node).
		Msg("Node has failed in leader election, retrying")

	go common.DoWithLabels(map[string]string{
		"oxia":     "shard-controller-retry-failed-follower",
		"shard":    fmt.Sprintf("%d", s.shard),
		"follower": node.Internal,
	}, func() {
		backOff := common.NewBackOffWithInitialInterval(ctx, 1*time.Second)

		_ = backoff.RetryNotify(func() error {
			err := s.fenceAndAddFollower(ctx, node)
			if status.Code(err) == server.CodeInvalidEpoch {
				// If we're receiving invalid epoch error, it would mean
				// there's already a new epoch generated and we don't have
				// to keep trying with this old epoch
				s.log.Warn().Err(err).
					Interface("follower", node).
					Int64("epoch", s.Epoch()).
					Msg("Failed to fence, invalid epoch. Stop trying")
				return nil
			}
			return err
		}, backOff, func(err error, duration time.Duration) {
			s.log.Warn().Err(err).
				Interface("follower", node).
				Int64("epoch", s.Epoch()).
				Dur("retry-after", duration).
				Msg("Failed to fence, retrying later")
		})
	})
}

func (s *shardController) fenceAndAddFollower(ctx context.Context, node ServerAddress) error {
	fr, err := s.fence(ctx, node)
	if err != nil {
		return err
	}

	s.Lock()
	leader := s.shardMetadata.Leader
	s.Unlock()
	if leader == nil {
		return errors.New("not leader is active on the shard")
	}

	if err = s.addFollower(*s.shardMetadata.Leader, node.Internal, &proto.EntryId{
		Epoch:  fr.Epoch,
		Offset: fr.Offset,
	}); err != nil {
		return err
	}

	s.log.Info().
		Interface("follower", node).
		Int64("epoch", fr.Epoch).
		Msg("Successfully rejoined the quorum")
	return nil
}

// Fence all the ensemble members in parallel and wait for
// a majority of them to reply successfully
func (s *shardController) fenceQuorum() (map[ServerAddress]*proto.EntryId, error) {
	ensembleSize := len(s.shardMetadata.Ensemble)
	majority := ensembleSize/2 + 1

	// Use a new context, so we can cancel the pending requests
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Channel to receive responses or errors from each server
	ch := make(chan struct {
		ServerAddress
		*proto.EntryId
		error
	})

	for _, sa := range s.shardMetadata.Ensemble {
		// We need to save the address because it gets modified in the loop
		serverAddress := sa
		go common.DoWithLabels(map[string]string{
			"oxia":  "shard-controller-leader-election",
			"shard": fmt.Sprintf("%d", s.shard),
			"node":  sa.Internal,
		}, func() {
			entryId, err := s.fence(ctx, serverAddress)
			if err != nil {
				s.log.Warn().Err(err).
					Str("node", serverAddress.Internal).
					Msg("Failed to fence node")
			} else {
				s.log.Info().
					Interface("server-address", serverAddress).
					Interface("entry-id", entryId).
					Msg("Processed fence response")
			}

			ch <- struct {
				ServerAddress
				*proto.EntryId
				error
			}{serverAddress, entryId, err}
		})
	}

	successResponses := 0
	totalResponses := 0

	res := make(map[ServerAddress]*proto.EntryId)
	var err error

	// Wait for a majority to respond
	for successResponses < majority && totalResponses < ensembleSize {
		r := <-ch

		totalResponses++
		if r.error == nil {
			successResponses++
			res[r.ServerAddress] = r.EntryId
		} else {
			err = multierr.Append(err, r.error)
		}
	}

	if successResponses < majority {
		return nil, errors.Wrap(err, "failed to fence shard")
	}

	// If we have already reached a quorum of successful responses, we can wait a
	// tiny bit more, to allow time for all the "healthy" nodes to respond.
	for err == nil && totalResponses < ensembleSize {
		select {
		case r := <-ch:
			totalResponses++
			if r.error == nil {
				res[r.ServerAddress] = r.EntryId
			} else {
				err = multierr.Append(err, r.error)
			}

		case <-time.After(quorumFencingGracePeriod):
			return res, nil
		}
	}

	return res, nil
}

func (s *shardController) fence(ctx context.Context, node ServerAddress) (*proto.EntryId, error) {
	res, err := s.rpc.Fence(ctx, node, &proto.FenceRequest{
		ShardId: s.shard,
		Epoch:   s.shardMetadata.Epoch,
	})
	if err != nil {
		return nil, err
	}

	return res.HeadIndex, nil
}

func (s *shardController) selectNewLeader(fenceResponses map[ServerAddress]*proto.EntryId) (
	leader ServerAddress, followers map[ServerAddress]*proto.EntryId) {
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
	followers = make(map[ServerAddress]*proto.EntryId)
	for a, e := range fenceResponses {
		if a != leader {
			followers[a] = e
		}
	}
	return leader, followers
}

func (s *shardController) becomeLeader(leader ServerAddress, followers map[ServerAddress]*proto.EntryId) error {
	followersMap := make(map[string]*proto.EntryId)
	for sa, e := range followers {
		followersMap[sa.Internal] = e
	}

	if _, err := s.rpc.BecomeLeader(s.ctx, leader, &proto.BecomeLeaderRequest{
		ShardId:           s.shard,
		Epoch:             s.shardMetadata.Epoch,
		ReplicationFactor: uint32(len(s.shardMetadata.Ensemble)),
		FollowerMaps:      followersMap,
	}); err != nil {
		return err
	}

	return nil
}

func (s *shardController) addFollower(leader ServerAddress, follower string, followerHeadIndex *proto.EntryId) error {
	if _, err := s.rpc.AddFollower(s.ctx, leader, &proto.AddFollowerRequest{
		ShardId:           s.shard,
		Epoch:             s.shardMetadata.Epoch,
		FollowerName:      follower,
		FollowerHeadIndex: followerHeadIndex,
	}); err != nil {
		return err
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
