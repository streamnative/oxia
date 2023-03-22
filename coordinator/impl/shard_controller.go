// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"oxia/common/metrics"
	"oxia/coordinator/model"
	"oxia/proto"
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

	HandleNodeFailure(failedNode model.ServerAddress)

	Term() int64
	Leader() *model.ServerAddress
	Status() model.ShardStatus
}

type shardController struct {
	sync.Mutex

	namespace     string
	shard         uint32
	shardMetadata model.ShardMetadata
	rpc           RpcProvider
	coordinator   Coordinator

	ctx    context.Context
	cancel context.CancelFunc

	currentElectionCtx    context.Context
	currentElectionCancel context.CancelFunc
	log                   zerolog.Logger

	leaderElectionLatency metrics.LatencyHistogram
	newTermQuorumLatency  metrics.LatencyHistogram
	becomeLeaderLatency   metrics.LatencyHistogram
	leaderElectionsFailed metrics.Counter
	termGauge             metrics.Gauge
}

func NewShardController(namespace string, shard uint32, shardMetadata model.ShardMetadata, rpc RpcProvider, coordinator Coordinator) ShardController {
	labels := metrics.LabelsForShard(namespace, shard)
	s := &shardController{
		namespace:     namespace,
		shard:         shard,
		shardMetadata: shardMetadata,
		rpc:           rpc,
		coordinator:   coordinator,
		log: log.With().
			Str("component", "shard-controller").
			Str("namespace", namespace).
			Uint32("shard", shard).
			Logger(),

		leaderElectionLatency: metrics.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		leaderElectionsFailed: metrics.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels),
		newTermQuorumLatency: metrics.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of nodes to a new term", labels),
		becomeLeaderLatency: metrics.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
	}

	s.termGauge = metrics.NewGauge("oxia_coordinator_term",
		"The term of the shard", "count", labels, func() int64 {
			return s.shardMetadata.Term
		})

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.log.Info().
		Interface("shard-metadata", s.shardMetadata).
		Msg("Started shard controller")

	if shardMetadata.Leader == nil || shardMetadata.Status != model.ShardStatusSteadyState {
		s.electLeaderWithRetries()
	} else {
		s.log.Info().
			Interface("current-leader", s.shardMetadata.Leader).
			Msg("There is already a node marked as leader on the shard, verifying")
		go s.verifyCurrentLeader(*shardMetadata.Leader)
	}
	return s
}

func (s *shardController) HandleNodeFailure(failedNode model.ServerAddress) {
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

func (s *shardController) verifyCurrentLeader(leader model.ServerAddress) {
	s.Lock()
	defer s.Unlock()

	status, err := s.rpc.GetStatus(s.ctx, leader, &proto.GetStatusRequest{ShardId: s.shard})

	if err != nil {
		s.log.Warn().Err(err).
			Interface("leader", leader).
			Msg("Failed to verify leader for shard. Start a new election")
	} else if status.Status != proto.ServingStatus_LEADER {
		s.log.Warn().
			Interface("leader", leader).
			Interface("status", status.Status).
			Msg("Node is not in leader status")
	} else if status.Term != s.shardMetadata.Term {
		s.log.Warn().
			Interface("leader", leader).
			Interface("node-term", status.Term).
			Interface("coordinator-term", s.shardMetadata.Term).
			Msg("Node has a wrong term")
	} else {
		s.log.Info().
			Interface("leader", leader).
			Msg("Leader looks ok. Do not trigger a new election for now")
		return
	}

	s.electLeaderWithRetries()
}

func (s *shardController) electLeaderWithRetries() {
	go common.DoWithLabels(map[string]string{
		"oxia":  "shard-controller-leader-election",
		"shard": fmt.Sprintf("%d", s.shard),
	}, func() {
		_ = backoff.RetryNotify(s.electLeader, common.NewBackOff(s.ctx),
			func(err error, duration time.Duration) {
				s.leaderElectionsFailed.Inc()
				s.log.Warn().Err(err).
					Dur("retry-after", duration).
					Msg("Leader election has failed, retrying later")
			})
	})
}

func (s *shardController) electLeader() error {
	timer := s.leaderElectionLatency.Timer()

	s.Lock()
	defer s.Unlock()

	if s.currentElectionCancel != nil {
		// Cancel any pending activity from the previous election
		s.currentElectionCancel()
	}

	s.shardMetadata.Status = model.ShardStatusElection
	s.shardMetadata.Leader = nil
	s.shardMetadata.Term++
	s.log.Info().
		Int64("term", s.shardMetadata.Term).
		Msg("Starting leader election")

	if err := s.coordinator.InitiateLeaderElection(s.namespace, s.shard, s.shardMetadata); err != nil {
		return err
	}

	// Send NewTerm to all the ensemble members
	fr, err := s.newTermQuorum()
	if err != nil {
		return err
	}

	newLeader, followers := s.selectNewLeader(fr)

	if s.log.Info().Enabled() {
		f := make([]struct {
			ServerAddress model.ServerAddress `json:"server-address"`
			EntryId       *proto.EntryId      `json:"entry-id"`
		}, 0)
		for sa, entryId := range followers {
			f = append(f, struct {
				ServerAddress model.ServerAddress `json:"server-address"`
				EntryId       *proto.EntryId      `json:"entry-id"`
			}{ServerAddress: sa, EntryId: entryId})
		}
		s.log.Info().
			Int64("term", s.shardMetadata.Term).
			Interface("new-leader", newLeader).
			Interface("followers", f).
			Msg("Successfully moved ensemble to a new term")
	}

	if err = s.becomeLeader(newLeader, followers); err != nil {
		return err
	}

	metadata := s.shardMetadata.Clone()
	metadata.Status = model.ShardStatusSteadyState
	metadata.Leader = &newLeader

	if err = s.coordinator.ElectedLeader(s.namespace, s.shard, metadata); err != nil {
		return err
	}

	s.shardMetadata = metadata

	s.log.Info().
		Int64("term", s.shardMetadata.Term).
		Interface("leader", s.shardMetadata.Leader).
		Msg("Elected new leader")

	defer timer.Done()
	s.keepFencingFailedFollowers(followers)
	return nil
}

func (s *shardController) keepFencingFailedFollowers(successfulFollowers map[model.ServerAddress]*proto.EntryId) {
	if len(successfulFollowers) == len(s.shardMetadata.Ensemble)-1 {
		s.log.Debug().
			Int64("term", s.shardMetadata.Term).
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

func (s *shardController) keepFencingFollower(ctx context.Context, node model.ServerAddress) {
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
			err := s.newTermAndAddFollower(ctx, node)
			if status.Code(err) == common.CodeInvalidTerm {
				// If we're receiving invalid term error, it would mean
				// there's already a new term generated, and we don't have
				// to keep trying with this old term
				s.log.Warn().Err(err).
					Interface("follower", node).
					Int64("term", s.Term()).
					Msg("Failed to newTerm, invalid term. Stop trying")
				return nil
			}
			return err
		}, backOff, func(err error, duration time.Duration) {
			s.log.Warn().Err(err).
				Interface("follower", node).
				Int64("term", s.Term()).
				Dur("retry-after", duration).
				Msg("Failed to newTerm, retrying later")
		})
	})
}

func (s *shardController) newTermAndAddFollower(ctx context.Context, node model.ServerAddress) error {
	fr, err := s.newTerm(ctx, node)
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
		Term:   fr.Term,
		Offset: fr.Offset,
	}); err != nil {
		return err
	}

	s.log.Info().
		Interface("follower", node).
		Int64("term", fr.Term).
		Msg("Successfully rejoined the quorum")
	return nil
}

// Send NewTerm to all the ensemble members in parallel and wait for
// a majority of them to reply successfully
func (s *shardController) newTermQuorum() (map[model.ServerAddress]*proto.EntryId, error) {
	timer := s.newTermQuorumLatency.Timer()

	ensembleSize := len(s.shardMetadata.Ensemble)
	majority := ensembleSize/2 + 1

	// Use a new context, so we can cancel the pending requests
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Channel to receive responses or errors from each server
	ch := make(chan struct {
		model.ServerAddress
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
			entryId, err := s.newTerm(ctx, serverAddress)
			if err != nil {
				s.log.Warn().Err(err).
					Str("node", serverAddress.Internal).
					Msg("Failed to newTerm node")
			} else {
				s.log.Info().
					Interface("server-address", serverAddress).
					Interface("entry-id", entryId).
					Msg("Processed newTerm response")
			}

			ch <- struct {
				model.ServerAddress
				*proto.EntryId
				error
			}{serverAddress, entryId, err}
		})
	}

	successResponses := 0
	totalResponses := 0

	res := make(map[model.ServerAddress]*proto.EntryId)
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
		return nil, errors.Wrap(err, "failed to newTerm shard")
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
			timer.Done()
			return res, nil
		}
	}

	timer.Done()
	return res, nil
}

func (s *shardController) newTerm(ctx context.Context, node model.ServerAddress) (*proto.EntryId, error) {
	res, err := s.rpc.NewTerm(ctx, node, &proto.NewTermRequest{
		Namespace: s.namespace,
		ShardId:   s.shard,
		Term:      s.shardMetadata.Term,
	})
	if err != nil {
		return nil, err
	}

	return res.HeadEntryId, nil
}

func (s *shardController) selectNewLeader(newTermResponses map[model.ServerAddress]*proto.EntryId) (
	leader model.ServerAddress, followers map[model.ServerAddress]*proto.EntryId) {
	// Select all the nodes that have the highest entry in the wal
	var currentMax int64 = -1
	var candidates []model.ServerAddress

	for addr, headEntryId := range newTermResponses {
		if headEntryId.Offset < currentMax {
			continue
		} else if headEntryId.Offset == currentMax {
			candidates = append(candidates, addr)
		} else {
			// Found a new max
			currentMax = headEntryId.Offset
			candidates = []model.ServerAddress{addr}
		}
	}

	// Select a random leader among the nodes with the highest entry in the wal
	leader = candidates[rand.Intn(len(candidates))]
	followers = make(map[model.ServerAddress]*proto.EntryId)
	for a, e := range newTermResponses {
		if a != leader {
			followers[a] = e
		}
	}
	return leader, followers
}

func (s *shardController) becomeLeader(leader model.ServerAddress, followers map[model.ServerAddress]*proto.EntryId) error {
	timer := s.leaderElectionLatency.Timer()

	followersMap := make(map[string]*proto.EntryId)
	for sa, e := range followers {
		followersMap[sa.Internal] = e
	}

	if _, err := s.rpc.BecomeLeader(s.ctx, leader, &proto.BecomeLeaderRequest{
		Namespace:         s.namespace,
		ShardId:           s.shard,
		Term:              s.shardMetadata.Term,
		ReplicationFactor: uint32(len(s.shardMetadata.Ensemble)),
		FollowerMaps:      followersMap,
	}); err != nil {
		return err
	}

	timer.Done()
	return nil
}

func (s *shardController) addFollower(leader model.ServerAddress, follower string, followerHeadEntryId *proto.EntryId) error {
	if _, err := s.rpc.AddFollower(s.ctx, leader, &proto.AddFollowerRequest{
		Namespace:           s.namespace,
		ShardId:             s.shard,
		Term:                s.shardMetadata.Term,
		FollowerName:        follower,
		FollowerHeadEntryId: followerHeadEntryId,
	}); err != nil {
		return err
	}

	return nil
}

func (s *shardController) Term() int64 {
	s.Lock()
	defer s.Unlock()
	return s.shardMetadata.Term
}

func (s *shardController) Leader() *model.ServerAddress {
	s.Lock()
	defer s.Unlock()
	return s.shardMetadata.Leader
}

func (s *shardController) Status() model.ShardStatus {
	s.Lock()
	defer s.Unlock()
	return s.shardMetadata.Status
}

func (s *shardController) Close() error {
	s.cancel()
	s.termGauge.Unregister()
	return nil
}
