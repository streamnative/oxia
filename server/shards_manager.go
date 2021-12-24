package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/proto"
	"sync"
)

type ShardsManager interface {
	io.Closer

	GetShardsAssignments(callback func(*proto.ShardsAssignments))

	UpdateClusterStatus(status *proto.ClusterStatus) error

	GetLeaderController(shardId uint32) (ShardLeaderController, error)
}

type shardsManager struct {
	mutex *sync.Mutex
	cond  *sync.Cond

	assignments  *proto.ShardsAssignments
	leading      map[uint32]ShardLeaderController
	following    map[uint32]ShardFollowerController
	identityAddr string

	log zerolog.Logger
}

func NewShardsManager(identityAddr string) ShardsManager {
	mutex := &sync.Mutex{}
	return &shardsManager{
		mutex: mutex,
		cond:  sync.NewCond(mutex),

		identityAddr: identityAddr,
		leading:      make(map[uint32]ShardLeaderController),
		following:    make(map[uint32]ShardFollowerController),
		log: log.With().
			Str("component", "shards-manager").
			Logger(),
	}
}

func (s *shardsManager) GetShardsAssignments(callback func(*proto.ShardsAssignments)) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.assignments != nil {
		callback(s.assignments)
	}

	oldAssignments := s.assignments
	for {
		s.cond.Wait()

		if oldAssignments != s.assignments {
			callback(s.assignments)
			oldAssignments = s.assignments
		}
	}
}

func (s *shardsManager) UpdateClusterStatus(status *proto.ClusterStatus) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Compare what we have and what we should have running in this node
	for _, shard := range status.GetShardsStatus() {
		if s.shouldLead(shard) {
			// We are leaders for this shard
			leaderCtrl, ok := s.leading[shard.Shard]
			if ok {
				s.log.Debug().
					Uint32("shard", shard.Shard).
					Msg("We are already leading, nothing to do")
			} else {
				followerCtrl, ok := s.following[shard.Shard]
				if ok {
					// We are being promoted from follower -> leader
					followerCtrl.Close()
					delete(s.following, shard.Shard)
				}

				leaderCtrl = NewShardLeaderController(shard.Shard, status.ReplicationFactor)
				s.leading[shard.Shard] = leaderCtrl
			}
		} else if s.shouldFollow(shard) {
			// We are followers for this shard
			followerCtrl, ok := s.following[shard.Shard]
			if ok {
				s.log.Debug().
					Uint32("shard", shard.Shard).
					Msg("We are already following, nothing to do")
			} else {
				leaderCtrl, ok := s.leading[shard.Shard]
				if ok {
					// We are being demoted from leader -> follower
					leaderCtrl.Close()
					delete(s.leading, shard.Shard)
				}

				followerCtrl = NewShardFollowerController(shard.Shard, shard.Leader.InternalUrl)
				s.following[shard.Shard] = followerCtrl
			}
		} else {
			// We should not be running this shard, close it if it's running
			if leaderCtrl, ok := s.leading[shard.Shard]; ok {
				leaderCtrl.Close()
			}

			if followerCtrl, ok := s.following[shard.Shard]; ok {
				followerCtrl.Close()
			}
		}
	}

	s.assignments = s.computeNewAssignments(status)
	return nil
}

func (s *shardsManager) computeNewAssignments(status *proto.ClusterStatus) *proto.ShardsAssignments {
	assignments := &proto.ShardsAssignments{
		Shards: make([]*proto.ShardAssignment, 0),
	}
	for _, shard := range status.GetShardsStatus() {
		assignments.Shards = append(assignments.Shards, &proto.ShardAssignment{
			ShardId: shard.Shard,
			Leader:  shard.Leader.PublicUrl,
		})
	}

	s.cond.Broadcast()

	return assignments
}

func (s *shardsManager) GetLeaderController(shardId uint32) (ShardLeaderController, error) {
	return NewShardLeaderController(shardId, 1), nil
}

func (s *shardsManager) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for shard, leaderCtrl := range s.leading {
		if err := leaderCtrl.Close(); err != nil {
			s.log.Error().
				Err(err).
				Uint32("shard", shard).
				Msg("Failed to shutdown leader controller")
		}
	}

	for shard, followerCtrl := range s.following {
		if err := followerCtrl.Close(); err != nil {
			s.log.Error().
				Err(err).
				Uint32("shard", shard).
				Msg("Failed to shutdown follower controller")
		}
	}

	return nil
}

func (s *shardsManager) shouldLead(shard *proto.ShardStatus) bool {
	return shard.Leader.InternalUrl == s.identityAddr
}

func (s *shardsManager) shouldFollow(shard *proto.ShardStatus) bool {
	for _, f := range shard.Followers {
		if f.InternalUrl == s.identityAddr {
			return true
		}
	}

	return false
}
