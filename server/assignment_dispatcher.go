package server

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
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

type shardAssignmentDispatcher struct {
	sync.Mutex
	initialized    bool
	shardKeyRouter proto.ShardKeyRouter
	assignments    map[uint32]*proto.ShardAssignment
	clients        map[int]Client
	nextClientId   int
	stopRecv       context.CancelFunc
}

var (
	ErrorNotInitialized = errors.New("oxia: server not initialized yet")
	ErrorUnknownRouter  = errors.New("oxia: unknown shard key router")
	ErrorChangedRouter  = errors.New("oxia: changing shard key router is not supported")
	ErrorShardSplitting = errors.New("oxia: shard splitting is not yet supported")
)

func (s *shardAssignmentDispatcher) AddClient(clientStream Client) error {
	s.Lock()
	defer s.Unlock()
	if !s.initialized {
		return ErrorNotInitialized
	}
	assignments := s.convertAssignments()
	err := clientStream.Send(assignments)
	if err != nil {
		return err
	}
	s.clients[s.nextClientId] = clientStream
	s.nextClientId++
	return nil
}

func (s *shardAssignmentDispatcher) Close() error {
	s.Lock()
	defer s.Unlock()
	if s.stopRecv != nil {
		s.stopRecv()
	}
	return nil
}

func (s *shardAssignmentDispatcher) Initialized() bool {
	s.Lock()
	defer s.Unlock()
	return s.initialized
}

func (s *shardAssignmentDispatcher) ShardAssignment(srv proto.OxiaControl_ShardAssignmentServer) error {
	s.Lock()
	_, s.stopRecv = context.WithCancel(srv.Context())
	s.Unlock()
	defer func() {
		s.Lock()
		err := srv.SendAndClose(&proto.CoordinationShardAssignmentsResponse{})
		if s.stopRecv != nil {
			s.stopRecv()
			s.stopRecv = nil
		}
		s.Unlock()
		if err != nil {
			log.Err(err).Msg("Error closing ShardAssignment stream")
		}
	}()
	for {
		request, err := srv.Recv()
		if err != nil {
			return err
		} else if request == nil {
			return nil
		} else if err := s.updateShardAssignment(request); err != nil {
			return err
		}
	}
}

func (s *shardAssignmentDispatcher) updateShardAssignment(request *proto.ShardAssignmentsResponse) error {
	s.Lock()
	defer s.Unlock()
	if request.ShardKeyRouter == proto.ShardKeyRouter_UNKNOWN {
		return ErrorUnknownRouter
	}

	if !s.initialized {
		s.shardKeyRouter = request.ShardKeyRouter

	}
	if s.shardKeyRouter != request.ShardKeyRouter {
		return ErrorChangedRouter
	}
	for _, assignment := range request.Assignments {
		shard := assignment.ShardId
		_, found := s.assignments[shard]
		if s.initialized && !found {
			return ErrorShardSplitting
		}
		s.assignments[shard] = assignment
	}

	for id, client := range s.clients {
		err := client.Send(request)
		if err != nil {
			log.Err(err).Msg("Error sending shard assignment update to client")
			delete(s.clients, id)
		}
	}

	s.initialized = true
	return nil
}

func (s *shardAssignmentDispatcher) convertAssignments() *proto.ShardAssignmentsResponse {
	assignments := make([]*proto.ShardAssignment, 0, len(s.assignments))
	for _, assignment := range s.assignments {
		assignments = append(assignments, assignment)
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
		shardKeyRouter: proto.ShardKeyRouter_UNKNOWN,
		assignments:    make(map[uint32]*proto.ShardAssignment),
		clients:        make(map[int]Client),
	}
}

func NewStandaloneShardAssignmentDispatcher(address string, numShards uint32) ShardAssignmentsDispatcher {
	assignmentDispatcher := &shardAssignmentDispatcher{
		initialized:    false,
		shardKeyRouter: proto.ShardKeyRouter_UNKNOWN,
		assignments:    make(map[uint32]*proto.ShardAssignment),
		clients:        make(map[int]Client),
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
