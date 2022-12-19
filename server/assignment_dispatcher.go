package server

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
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
	RegisterForUpdates(client Client) error
}

type shardAssignmentDispatcher struct {
	sync.Mutex
	assignments  *proto.ShardAssignmentsResponse
	clients      map[int64]chan *proto.ShardAssignmentsResponse
	nextClientId int64

	ctx    context.Context
	cancel context.CancelFunc

	log zerolog.Logger
}

func (s *shardAssignmentDispatcher) RegisterForUpdates(clientStream Client) error {
	s.Lock()
	initialAssignments := s.assignments
	if initialAssignments == nil {
		s.Unlock()
		return ErrorNotInitialized
	}

	clientCh := make(chan *proto.ShardAssignmentsResponse)
	clientId := s.nextClientId
	s.nextClientId++

	s.clients[clientId] = clientCh
	s.Unlock()

	// Send initial assignments
	err := clientStream.Send(initialAssignments)
	if err != nil {
		s.Lock()
		delete(s.clients, clientId)
		s.Unlock()
		return err
	}

	for {
		select {
		case assignments := <-clientCh:
			if assignments == nil {
				return ErrorCancelled
			}

			err := clientStream.Send(assignments)
			if err != nil {
				if status.Code(err) != codes.Canceled {
					peer, _ := peer.FromContext(clientStream.Context())
					s.log.Warn().Err(err).
						Str("client", peer.Addr.String()).
						Msg("Failed to send shard assignment update to client")
				}
				s.Lock()
				delete(s.clients, clientId)
				s.Unlock()
				return err
			}

		case <-clientStream.Context().Done():
			// The client has disconnected or timed out
			s.Lock()
			delete(s.clients, clientId)
			s.Unlock()
			return nil

		case <-s.ctx.Done():
			// the server is closing
			return nil
		}
	}
}

func (s *shardAssignmentDispatcher) Close() error {
	s.cancel()
	return nil
}

func (s *shardAssignmentDispatcher) Initialized() bool {
	s.Lock()
	defer s.Unlock()
	return s.assignments != nil
}

func (s *shardAssignmentDispatcher) ShardAssignment(stream proto.OxiaControl_ShardAssignmentServer) error {
	ch := make(chan error)
	go common.DoWithLabels(map[string]string{
		"oxia": "receive-shards-assignments",
	}, func() { s.handleServerStream(stream, ch) })

	select {
	case err := <-ch:
		return err

	case <-s.ctx.Done():
		// Server is closing
		return nil
	}
}

func (s *shardAssignmentDispatcher) handleServerStream(stream proto.OxiaControl_ShardAssignmentServer, ch chan error) {
	for {
		request, err := stream.Recv()
		if err != nil {
			ch <- err
			return
		} else if request == nil {
			// The stream is already closing
			close(ch)
			return
		} else if err := s.updateShardAssignment(request); err != nil {
			ch <- err
			return
		}
	}
}

func (s *shardAssignmentDispatcher) updateShardAssignment(assignments *proto.ShardAssignmentsResponse) error {
	s.Lock()
	defer s.Unlock()

	s.assignments = assignments

	// Update all the clients, without getting stuck if any client is not responsive
	for id, clientCh := range s.clients {
		select {
		case clientCh <- assignments:
			// Good, we were able to pass the update to the client

		default:
			// The client is not responsive, cut it off
			close(clientCh)
			delete(s.clients, id)
		}
	}

	return nil
}

func NewShardAssignmentDispatcher() ShardAssignmentsDispatcher {
	s := &shardAssignmentDispatcher{
		assignments: nil,
		clients:     make(map[int64]chan *proto.ShardAssignmentsResponse),
		log: log.With().
			Str("component", "shard-assignment-dispatcher").
			Logger(),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

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
