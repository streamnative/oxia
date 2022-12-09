package server

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/peer"
	"io"
	"math"
	"oxia/common"
	"oxia/proto"
	"sync"
)

type Client interface {
	Send(*proto.ShardAssignmentsResponse) error

	Context() context.Context
}

type ShardAssignmentsDispatcher interface {
	io.Closer
	Initialized() bool
	ShardAssignment(stream proto.OxiaControl_ShardAssignmentServer) error
	AddClient(client Client) error
}

type shardAssignmentDispatcher struct {
	sync.Mutex
	assignments  *proto.ShardAssignmentsResponse
	clients      map[int]Client
	nextClientId int
	closeCh      chan error
	log          zerolog.Logger
}

var (
	ErrorNotInitialized = errors.New("oxia: server not initialized yet")
)

func (s *shardAssignmentDispatcher) AddClient(clientStream Client) error {
	s.Lock()
	defer s.Unlock()
	if s.assignments == nil {
		return ErrorNotInitialized
	}
	err := clientStream.Send(s.assignments)
	if err != nil {
		return err
	}
	s.clients[s.nextClientId] = clientStream
	s.nextClientId++
	return nil
}

func (s *shardAssignmentDispatcher) Close() error {
	s.closeChannel(nil)
	return nil
}

func (s *shardAssignmentDispatcher) Initialized() bool {
	s.Lock()
	defer s.Unlock()
	return s.assignments != nil
}

func (s *shardAssignmentDispatcher) ShardAssignment(stream proto.OxiaControl_ShardAssignmentServer) error {
	go common.DoWithLabels(map[string]string{
		"oxia": "receive-shards-assignments",
	}, func() { s.handleServerStream(stream) })

	return <-s.closeCh
}

func (s *shardAssignmentDispatcher) handleServerStream(stream proto.OxiaControl_ShardAssignmentServer) {
	for {
		request, err := stream.Recv()
		if err != nil {
			s.closeChannel(err)
			return
		} else if request == nil {
			// The stream is already closing
			return
		} else if err := s.updateShardAssignment(request); err != nil {
			s.closeChannel(err)
			return
		}
	}
}

func (s *shardAssignmentDispatcher) closeChannel(err error) {
	s.Lock()
	defer s.Unlock()

	if s.closeCh != nil {
		s.closeCh <- err
		close(s.closeCh)
		s.closeCh = nil
	}
}

func (s *shardAssignmentDispatcher) updateShardAssignment(assignments *proto.ShardAssignmentsResponse) error {
	s.Lock()
	defer s.Unlock()

	s.assignments = assignments

	for id, client := range s.clients {
		err := client.Send(assignments)
		if err != nil {
			peer, _ := peer.FromContext(client.Context())
			s.log.Warn().Err(err).
				Str("client", peer.Addr.String()).
				Msg("Failed to send shard assignment update to client")
			delete(s.clients, id)
		}
	}

	return nil
}

func NewShardAssignmentDispatcher() ShardAssignmentsDispatcher {
	s := &shardAssignmentDispatcher{
		assignments: nil,
		clients:     make(map[int]Client),
		closeCh:     make(chan error),
		log: log.With().
			Str("component", "shard-assignment-dispatcher").
			Logger(),
	}

	return s
}

func NewStandaloneShardAssignmentDispatcher(address string, numShards uint32) ShardAssignmentsDispatcher {
	assignmentDispatcher := NewShardAssignmentDispatcher().(*shardAssignmentDispatcher)
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
