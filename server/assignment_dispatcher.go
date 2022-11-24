package server

import (
	"github.com/pkg/errors"
	"io"
	"math"
	"oxia/proto"
	"sync"
)

type Client interface {
	Send(*proto.ShardAssignmentsResponse) error
}

type ShardAssignmentsDispatcher interface {
	io.Closer
	Initialized() bool
	ShardAssignment(proto.OxiaControl_ShardAssignmentServer) error
	AddClient(Client) error
}

type shardAssignment struct {
	leader              string
	lowerBoundInclusive uint32
	upperBoundExclusive uint32
}

type shardAssignmentDispatcher struct {
	sync.Mutex
	initialized    bool
	closed         bool
	shardKeyRouter proto.ShardKeyRouter
	assignments    map[uint32]shardAssignment
	clients        []Client
}

func (s *shardAssignmentDispatcher) AddClient(clientStream Client) error {
	s.Lock()
	defer s.Unlock()
	if !s.initialized {
		return errors.New("oxia: server not initialized yet")
	}
	assignments := s.convertAssignments()
	err := clientStream.Send(assignments)
	if err != nil {
		return err
	}
	s.clients = append(s.clients, clientStream)
	return nil
}

func (s *shardAssignmentDispatcher) Close() error {
	s.Lock()
	defer s.Unlock()
	s.closed = true
	return nil
}

func (s *shardAssignmentDispatcher) Initialized() bool {
	s.Lock()
	defer s.Unlock()
	return s.initialized
}

func (s *shardAssignmentDispatcher) ShardAssignment(srv proto.OxiaControl_ShardAssignmentServer) error {
	for {
		request, err := srv.Recv()
		if err == nil {
			return err
		} else if request == nil {
			return nil
		} else if err := s.updateShardAssignment(request); err != nil {
			return err
		}
		s.Lock()
		closed := s.closed
		s.Unlock()
		if closed {
			return nil
		}
	}
}

func (s *shardAssignmentDispatcher) updateShardAssignment(request *proto.ShardAssignmentsResponse) error {
	s.Lock()
	defer s.Unlock()
	if request.ShardKeyRouter == proto.ShardKeyRouter_UNKNOWN {
		return errors.New("oxia: unknown shard key router")
	}

	if !s.initialized {
		s.shardKeyRouter = request.ShardKeyRouter

	}
	if s.shardKeyRouter != request.ShardKeyRouter {
		return errors.New("oxia: changing shard key router is not supported")
	}
	for _, assignment := range request.Assignments {
		shard := assignment.ShardId
		current, found := s.assignments[shard]
		if s.initialized && !found {
			return errors.New("oxia: shard splitting is not supported")
		}
		current.leader = assignment.Leader
		current.lowerBoundInclusive = assignment.GetInt32HashRange().MinHashInclusive
		current.upperBoundExclusive = assignment.GetInt32HashRange().MaxHashExclusive
		s.assignments[shard] = current
	}

	for idx, client := range s.clients {
		err := client.Send(request)
		if err != nil {
			s.clients = removeAt(s.clients, idx)
			return err
		}
	}

	s.initialized = true
	return nil
}

func removeAt[T any](slice []T, idx int) []T {
	if idx == len(slice)-1 {
		return slice[:idx]
	} else {
		return append(slice[:idx], slice[idx+1:]...)
	}
}

func (s *shardAssignmentDispatcher) convertAssignments() *proto.ShardAssignmentsResponse {
	assignments := make([]*proto.ShardAssignment, 0, len(s.assignments))
	for shard, assignment := range s.assignments {
		assignments = append(assignments, &proto.ShardAssignment{
			ShardId: shard,
			Leader:  assignment.leader,
			ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
				Int32HashRange: &proto.Int32HashRange{
					MinHashInclusive: assignment.lowerBoundInclusive,
					MaxHashExclusive: assignment.upperBoundExclusive,
				}},
		})
	}
	result := &proto.ShardAssignmentsResponse{
		Assignments:    assignments,
		ShardKeyRouter: proto.ShardKeyRouter(s.shardKeyRouter.Number()),
	}
	return result
}

func NewShardAssignmentDispatcher() ShardAssignmentsDispatcher {
	return &shardAssignmentDispatcher{
		initialized:    false,
		closed:         false,
		shardKeyRouter: proto.ShardKeyRouter_UNKNOWN,
		assignments:    make(map[uint32]shardAssignment, 1000),
		clients:        make([]Client, 0, 1000),
	}
}

func NewStandaloneShardAssignmentDispatcher(address string, numShards uint32) ShardAssignmentsDispatcher {
	assignmentDispatcher := &shardAssignmentDispatcher{
		initialized:    false,
		closed:         false,
		shardKeyRouter: proto.ShardKeyRouter_UNKNOWN,
		assignments:    make(map[uint32]shardAssignment, 1000),
		clients:        make([]Client, 0, 1000),
	}
	res := &proto.ShardAssignmentsResponse{
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}

	bucketSize := math.MaxUint32 / numShards

	for i := uint32(0); i < numShards; i++ {
		upperBound := (i + 1) * bucketSize
		if i == numShards-1 {
			upperBound = math.MaxUint32
		}
		res.Assignments = append(res.Assignments, &proto.ShardAssignment{
			ShardId: i,
			Leader:  address,
			ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
				Int32HashRange: &proto.Int32HashRange{
					MinHashInclusive: i * bucketSize,
					MaxHashExclusive: upperBound,
				},
			},
		})
	}
	err := assignmentDispatcher.updateShardAssignment(res)
	if err != nil {
		panic(err)
	}
	return assignmentDispatcher
}
