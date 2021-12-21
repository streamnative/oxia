package main

import (
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
	"sync"
)

type ShardsManager interface {
	io.Closer

	GetShardsAssignments(callback func(*proto.ShardsAssignments) ())

	UpdateClusterStatus(status *proto.ClusterStatus) error

	GetLeaderController(shardId uint32) (ShardLeaderController, error)
}

type shardsManager struct {
	mutex *sync.Mutex
	cond  *sync.Cond

	assignments    *proto.ShardsAssignments
	leading        sync.Map
	following      sync.Map
	connectionPool common.ConnectionPool
}

func NewShardsManager(connectionPool common.ConnectionPool) ShardsManager {
	mutex := &sync.Mutex{}
	return &shardsManager{
		mutex:          mutex,
		cond:           sync.NewCond(mutex),
		connectionPool: connectionPool,
	}
}

func (s *shardsManager) GetShardsAssignments(callback func(*proto.ShardsAssignments) ()) {
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

	log.Info().
		Msg("UpdateClusterStatus")
	return nil
}

func computeNewAssignments(status *proto.ClusterStatus) *proto.ShardsAssignments {
	//assignements := &proto.ShardsAssignments{
	//	Shards: make([]*proto.ShardStatus, 0),
	//}
	//for _, shard := range status.GetShardsStatus() {
	//	assignements
	//}
	//
	//return assignements
	panic(nil)
}

func (s *shardsManager) GetLeaderController(shardId uint32) (ShardLeaderController, error) {
	return NewShardLeaderController(shardId, 1), nil
}

func (s *shardsManager) Close() error {
	panic("implement me")
}
