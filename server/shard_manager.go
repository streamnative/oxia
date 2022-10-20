package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"oxia/common"
	"oxia/coordination"
	"oxia/proto"
	"time"
)

// ShardManager manages one shard.
// Its methods are distinct RPCs from the TLA spec. (e.g. Fence)
// Implemented by [shardManager] below
type ShardManager interface {
	io.Closer

	Fence(req *coordination.FenceRequest) (*coordination.FenceResponse, error)
	BecomeLeader(*coordination.BecomeLeaderRequest) (*coordination.BecomeLeaderResponse, error)
	AddFollower(*coordination.AddFollowerRequest) (*emptypb.Empty, error)
	Truncate(*coordination.TruncateRequest) (*coordination.TruncateResponse, error)
	PrepareReconfig(*coordination.PrepareReconfigRequest) (*coordination.PrepareReconfigResponse, error)
	Snapshot(*coordination.SnapshotRequest) (*coordination.SnapshotResponse, error)
	CommitReconfig(*coordination.CommitReconfigRequest) (*coordination.CommitReconfigResponse, error)
	Put(op *proto.PutOp) (*proto.Stat, error)
	AddEntries(srv coordination.OxiaCoordination_AddEntriesServer) (any, error)
}

// Command is the representation of the work a [ShardManager] is supposed to do when it gets a request and a channel for the response.
// Ideally it would be generic with the type parameter being the response type
type Command struct {
	execute      func() (any, bool, error)
	responseChan chan any
}

func newCommand(execute func() (any, bool, error)) *Command {
	return &Command{
		// Command cannot be generic, because make(chan T, 1) does not compile
		responseChan: make(chan any, 1),
		execute:      execute,
	}
}

type Status int16

const (
	NotMember Status = iota
	Leader
	Follower
	Fenced
)

type CursorStatus int16

const (
	Attached CursorStatus = iota
	PendingTruncate
	PendingRemoval
)

type cursor struct {
	status        CursorStatus
	lastPushed    *coordination.EntryId
	lastConfirmed *coordination.EntryId
}

type shardManager struct {
	shard              uint32
	epoch              uint64
	replicationFactor  uint32
	leader             string
	commitIndex        *coordination.EntryId
	headIndex          *coordination.EntryId
	followCursor       map[string]*cursor
	reconfigInProgress bool
	status             Status

	wal             Wal
	commandChannel  chan *Command // Individual requests are sent to this channel for serial processing
	clientPool      common.ClientPool
	identityAddress string
	log             zerolog.Logger
}

func NewShardManager(shard uint32, identityAddress string, pool common.ClientPool) ShardManager {
	sm := &shardManager{
		shard:              shard,
		epoch:              0,
		replicationFactor:  0,
		leader:             "",
		commitIndex:        nil,
		headIndex:          nil,
		followCursor:       make(map[string]*cursor),
		reconfigInProgress: false,
		status:             NotMember,

		wal:             NewWal(shard),
		commandChannel:  make(chan *Command, 8),
		clientPool:      pool,
		identityAddress: identityAddress,
		log: log.With().
			Str("component", "shard-manager").
			Uint32("shard", shard).
			Logger(),
	}

	sm.log.Info().
		Uint32("replicationFactor", sm.replicationFactor).
		Msg("Start managing shard")
	go sm.run()

	return sm
}

func enqueueCommandAndWaitForResponse[T any](s *shardManager, f func() (T, bool, error)) T {
	command := newCommand(func() (any, bool, error) {
		r, e, err := f()
		return r, e, err
	})
	s.commandChannel <- command
	response := <-command.responseChan
	// TODO get rid of type assertion. It's ugly
	return response.(T)
}

func (s *shardManager) checkEpochLaterIn(req interface{ GetEpoch() uint64 }) error {
	if req.GetEpoch() <= s.epoch {
		return status.Errorf(codes.FailedPrecondition, "Got old epoch %d, when at %d", req.GetEpoch(), s.epoch)
	}
	return nil
}

func (s *shardManager) checkEpochEqualIn(req interface{ GetEpoch() uint64 }) error {
	if req.GetEpoch() != s.epoch {
		return status.Errorf(codes.FailedPrecondition, "Got clashing epoch %d, when at %d", req.GetEpoch(), s.epoch)
	}
	return nil
}

// Fence enqueues a call to fenceSync and waits for the response
func (s *shardManager) Fence(req *coordination.FenceRequest) (*coordination.FenceResponse, error) {
	response := enqueueCommandAndWaitForResponse[*coordination.FenceResponse](s, func() (*coordination.FenceResponse, bool, error) {
		resp, err := s.fenceSync(req)
		return resp, false, err
	})
	return response, nil
}

// fenceSync, like all *Sync methods, is called by run method serially
/*
  Node handles a fence request

  A node receives a fencing request, fences itself and responds
  with its head index.

  When a node is fenced it cannot:
  - accept any writes from a client.
  - accept add entry requests from a leader.
  - send any entries to followers if it was a leader.

  Any existing follow cursors are destroyed as is any state
  regarding reconfigurations.
*/
func (s *shardManager) fenceSync(req *coordination.FenceRequest) (*coordination.FenceResponse, error) {
	if err := s.checkEpochLaterIn(req); err != nil {
		// TODO what to do? Error out or Respond with newer epoch? What if equal?
		return nil, err
	}
	s.epoch = req.GetEpoch()
	s.status = Fenced
	s.replicationFactor = 0
	s.followCursor = make(map[string]*cursor)
	// TODO stop replicating in AddEntries
	s.reconfigInProgress = false
	return &coordination.FenceResponse{
		Epoch:     s.epoch,
		HeadIndex: s.headIndex,
	}, nil
}

func (s *shardManager) BecomeLeader(req *coordination.BecomeLeaderRequest) (*coordination.BecomeLeaderResponse, error) {
	response := enqueueCommandAndWaitForResponse[*coordination.BecomeLeaderResponse](s, func() (*coordination.BecomeLeaderResponse, bool, error) {
		resp, err := s.becomeLeaderSync(req)
		return resp, false, err
	})
	return response, nil
}

func (s *shardManager) getHighestEntryIdOfEpoch() (*coordination.EntryId, error) {
	entryId, err := s.wal.LastEntryIdUptoEpoch(s.epoch)
	return entryId, err
}

func (s *shardManager) needsTruncation(headIndex *coordination.EntryId) bool {
	return headIndex.Epoch != s.headIndex.Epoch || headIndex.Offset > s.headIndex.Offset
}

func (s *shardManager) getCursor(headIndex *coordination.EntryId) *cursor {
	if s.needsTruncation(headIndex) {
		return &cursor{
			status:        PendingTruncate,
			lastPushed:    nil,
			lastConfirmed: nil,
		}
	} else {
		return &cursor{
			status:        Attached,
			lastPushed:    headIndex,
			lastConfirmed: headIndex,
		}
	}
}

func (s *shardManager) getCursors(followers []*coordination.BecomeLeaderRequest_FollowerEntry) map[string]*cursor {
	cursors := make(map[string]*cursor)
	for _, kv := range followers {
		cursors[kv.Key.InternalUrl] = s.getCursor(kv.Value)
	}
	return cursors
}

func (s *shardManager) sendTruncateRequest(k string, entryId *coordination.EntryId) error {
	rpc, err := s.clientPool.GetInternalRpc(k)
	if err != nil {
		return err
	}
	go func() {

		_, err2 := rpc.(coordination.OxiaCoordinationClient).Truncate( /*TODO*/ nil, &coordination.TruncateRequest{
			Epoch:     s.epoch,
			HeadIndex: entryId,
		})
		if err2 != nil {
			log.Error().Err(err2).Msg("Got error sending truncateRequest")
		}
		/*TODO call something like truncateResponseSync(TruncateResponse) in run */
	}()
	return nil
}

func (s *shardManager) becomeLeaderSync(req *coordination.BecomeLeaderRequest) (*coordination.BecomeLeaderResponse, error) {
	if err := s.checkEpochEqualIn(req); err != nil {
		// TODO what to do? Error out or Respond with own epoch?
		return nil, err
	}
	s.status = Leader
	s.leader = s.identityAddress
	s.replicationFactor = req.GetReplicationFactor()
	s.followCursor = s.getCursors(req.GetFollowerMap())
	highestEntryOfEpoch, err := s.getHighestEntryIdOfEpoch()
	if err != nil {
		return nil, err
	}
	for k, v := range s.followCursor {
		if v.status == PendingTruncate {
			err := s.sendTruncateRequest(k, highestEntryOfEpoch)
			if err != nil {
				return nil, err
			}

		}
	}
	return &coordination.BecomeLeaderResponse{Epoch: req.GetEpoch()}, nil
}

func (s *shardManager) AddFollower(req *coordination.AddFollowerRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddFollower not implemented")
}
func (s *shardManager) Truncate(req *coordination.TruncateRequest) (*coordination.TruncateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Truncate not implemented")
}
func (s *shardManager) AddEntries(srv coordination.OxiaCoordination_AddEntriesServer) (any, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddEntries not implemented")
}
func (s *shardManager) PrepareReconfig(req *coordination.PrepareReconfigRequest) (*coordination.PrepareReconfigResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PrepareReconfig not implemented")
}
func (s *shardManager) Snapshot(req *coordination.SnapshotRequest) (*coordination.SnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Snapshot not implemented")
}
func (s *shardManager) CommitReconfig(req *coordination.CommitReconfigRequest) (*coordination.CommitReconfigResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CommitReconfig not implemented")
}

func (s *shardManager) Put(op *proto.PutOp) (*proto.Stat, error) {
	s.log.Debug().
		Interface("op", op).
		Msg("Put operation")

	return &proto.Stat{
		Version:           1,
		CreatedTimestamp:  uint64(time.Now().Unix()),
		ModifiedTimestamp: uint64(time.Now().Unix()),
	}, nil
}

// run takes commands from the queue and executes them serially
func (s *shardManager) run() {
	for {
		command := <-s.commandChannel
		response, exit, err := command.execute()
		if response != nil {
			command.responseChan <- response
		}
		if err != nil {
			log.Error().Err(err).Msg("Error executing command.")
		}
		if exit || err != nil {
			log.Info().Msg("Shard manager closes.")
			break
		}
	}
}

func (s *shardManager) Close() error {
	s.log.Info().Msg("Closing shard manager")
	s.commandChannel <- newCommand(func() (any, bool, error) {
		s.status = NotMember
		return nil, true, nil
	})

	return s.wal.Close()
}


