package server

import (
	"encoding/json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"oxia/common"
	"oxia/coordination"
	"oxia/proto"
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
	Write(op *proto.PutOp) (*proto.Stat, error)
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

func sendRequestAndProcessResponse[RESP any](s *shardManager, target string, send func(client coordination.OxiaCoordinationClient) (RESP, error), process func(RESP) (bool, error)) error {
	rpc, err := s.clientPool.GetInternalRpc(target)
	if err != nil {
		return err
	}
	go func() {

		/*TODO create context with shard metadata and maybe timeout*/
		resp, err2 := send(rpc)
		if err2 != nil {
			log.Error().Err(err2).Msg("Got error sending truncateRequest")
		}
		command := newCommand(func() (any, bool, error) {
			exit, err3 := process(resp)
			return nil, exit, err3
		})
		s.commandChannel <- command
	}()
	return nil
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

func (s *shardManager) checkStatus(expected Status) error {
	if s.status != expected {
		return status.Errorf(codes.FailedPrecondition, "Received message in the wrong state. In %s, should be %s.", s.status, expected)
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

func (s *shardManager) sendTruncateRequest(target string, targetEpoch uint64) error {
	headIndex, err := s.wal.LastEntryIdUptoEpoch(targetEpoch)
	if err != nil {
		return err
	}
	send := func(rpc coordination.OxiaCoordinationClient) (*coordination.TruncateResponse, error) {
		resp, err2 := rpc.Truncate(nil, &coordination.TruncateRequest{
			Epoch:     s.epoch,
			HeadIndex: headIndex,
		})
		return resp, err2
	}
	process := func(resp *coordination.TruncateResponse) (bool, error) {
		exit, err2 := s.truncateResponseSync(target, resp)
		return exit, err2
	}
	return sendRequestAndProcessResponse[*coordination.TruncateResponse](s, target, send, process)
}

/*
truncateResponseSync: Leader handles a truncate response.

The leader now activates the follow cursor as the follower
log is now ready for replication.
*/
func (s *shardManager) truncateResponseSync(target string, resp *coordination.TruncateResponse) (bool, error) {
	if err := s.checkStatus(Leader); err != nil {
		return false, err
	}
	if err := s.checkEpochEqualIn(resp); err != nil {
		return false, err
	}
	s.followCursor[target] = &cursor{
		status:        Attached,
		lastPushed:    resp.HeadIndex,
		lastConfirmed: resp.HeadIndex,
	}
	// TODO start replication (send AppendEntries)

	return false, nil
}

/*
Node handles a Become Leader request

The node inspects the head index of each follower and
compares it to its own head index, and then either:
  - Attaches a follow cursor for the follower the head indexes
    have the same epoch, but the follower offset is lower or equal.
  - Sends a truncate request to the follower if its head
    index epoch does not match the leader's head index epoch or has
    a higher offset.
    The leader finds the highest entry id in its log prefix (of the
    follower head index) and tells the follower to truncate its log
    to that entry.

Key points:
  - The election only requires a majority to complete and so the
    Become Leader request will likely only contain a majority,
    not all the nodes.
  - No followers in the Become Leader message "follower map" will
    have a higher head index than the leader (as the leader was
    chosen because it had the highest head index of the majority
    that responded to the fencing requests first). But as the leader
    receives more fencing responses from the remaining minority,
    the new leader will be informed of these followers and it is
    possible that their head index is higher than the leader and
    therefore need truncating.
*/
func (s *shardManager) becomeLeaderSync(req *coordination.BecomeLeaderRequest) (*coordination.BecomeLeaderResponse, error) {
	if err := s.checkEpochEqualIn(req); err != nil {
		return nil, err
	}
	s.status = Leader
	s.leader = s.identityAddress
	s.replicationFactor = req.GetReplicationFactor()
	s.followCursor = s.getCursors(req.GetFollowerMap())

	for k, v := range s.followCursor {
		if v.status == PendingTruncate {
			err := s.sendTruncateRequest(k, s.epoch)
			if err != nil {
				return nil, err
			}

		}
	}
	return &coordination.BecomeLeaderResponse{Epoch: req.GetEpoch()}, nil
}
func (s *shardManager) Truncate(req *coordination.TruncateRequest) (*coordination.TruncateResponse, error) {
	response := enqueueCommandAndWaitForResponse[*coordination.TruncateResponse](s, func() (*coordination.TruncateResponse, bool, error) {
		resp, err := s.truncateSync(req)
		return resp, false, err
	})
	return response, nil
}

/*
Node handles a Truncate request

A node that receives a truncate request knows that it
has been selected as a follower. It truncates its log
to the indicates entry id, updates its epoch and changes
to a Follower.
*/
func (s *shardManager) truncateSync(req *coordination.TruncateRequest) (*coordination.TruncateResponse, error) {
	if err := s.checkStatus(Fenced); err != nil {
		return nil, err
	}
	if err := s.checkEpochEqualIn(req); err != nil {
		return nil, err
	}
	s.status = Follower
	s.epoch = req.Epoch
	s.leader = "" // TODO msg.source_node
	headEntryId, err := s.wal.TruncateLog(req.HeadIndex)
	if err != nil {
		return nil, err
	}
	s.headIndex = headEntryId
	// IMO this should have been already done when we got fenced
	s.followCursor = make(map[string]*cursor)

	return &coordination.TruncateResponse{
		Epoch:     req.Epoch,
		HeadIndex: headEntryId,
	}, nil
}

func (s *shardManager) AddFollower(req *coordination.AddFollowerRequest) (*emptypb.Empty, error) {
	response := enqueueCommandAndWaitForResponse[*emptypb.Empty](s, func() (*emptypb.Empty, bool, error) {
		resp, err := s.addFollowerSync(req)
		return resp, false, err
	})
	return response, nil
}

/*
Leader handles an Add Follower request

The leader creates a cursor and will send a truncate
request to the follower if their log might need
truncating first.
*/
func (s *shardManager) addFollowerSync(req *coordination.AddFollowerRequest) (*emptypb.Empty, error) {
	if err := s.checkStatus(Leader); err != nil {
		return nil, err
	}
	if err := s.checkEpochEqualIn(req); err != nil {
		return nil, err
	}
	if _, ok := s.followCursor[req.Follower.InternalUrl]; ok {
		return nil, status.Errorf(codes.FailedPrecondition, "Follower %s already exists", req.Follower.InternalUrl)
	}

	s.followCursor[req.Follower.InternalUrl] = s.getCursor(req.HeadIndex)
	if s.needsTruncation(req.HeadIndex) {
		err := s.sendTruncateRequest(req.Follower.InternalUrl, req.HeadIndex.Epoch)
		if err != nil {
			return nil, err
		}
	}

	return nil, status.Errorf(codes.Unimplemented, "method AddFollower not implemented")
}

func (s *shardManager) Write(op *proto.PutOp) (*proto.Stat, error) {
	response := enqueueCommandAndWaitForResponse[*proto.Stat](s, func() (*proto.Stat, bool, error) {
		resp, err := s.writeSync(op)
		return resp, false, err
	})
	return response, nil
}

func serializeOp(op *proto.PutOp) ([]byte, error) {
	// TODO reasonable serialization
	val, err := json.Marshal(map[string]any{
		"type": "Put",
		"op":   op,
	})
	return val, err
}

/*
Write A client writes an entry to the leader

	A client writes a value from Values to a leader node
	if that value has not previously been written. The leader adds
	the entry to its log, updates its head_index.
*/
func (s *shardManager) writeSync(op *proto.PutOp) (*proto.Stat, error) {
	s.log.Debug().
		Interface("op", op).
		Msg("Put operation")

	if err := s.checkStatus(Leader); err != nil {
		return nil, err
	}
	entryId := &coordination.EntryId{
		Epoch:  s.epoch,
		Offset: s.headIndex.Offset + 1,
	}
	value, err := serializeOp(op)
	if err != nil {
		return nil, err
	}
	logEntry := &coordination.LogEntry{
		EntryId: entryId,
		Value:   value,
	}

	err = s.wal.Append(logEntry)
	if err != nil {
		return nil, err
	}
	s.headIndex = entryId
	// TODO replication
	// TODO respond when majority acknowledges
	return nil, nil

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
