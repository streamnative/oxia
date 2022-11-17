package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"oxia/common"
	"oxia/proto"
	wal "oxia/server/wal"
	"time"
)

type ShardManager interface {
	io.Closer

	Fence(req *proto.FenceRequest) (*proto.FenceResponse, error)
	BecomeLeader(*proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	AddFollower(*proto.AddFollowerRequest) (*proto.CoordinationEmpty, error)
	Truncate(*proto.TruncateRequest) (*proto.TruncateResponse, error)
	Write(op *proto.PutOp) (*proto.Stat, error)
	AddEntries(proto.OxiaLogReplication_AddEntriesServer) error
	// Later
	PrepareReconfig(*proto.PrepareReconfigRequest) (*proto.PrepareReconfigResponse, error)
	Snapshot(*proto.SnapshotRequest) (*proto.SnapshotResponse, error)
	CommitReconfig(*proto.CommitReconfigRequest) (*proto.CommitReconfigResponse, error)
}

// Command is the representation of the work a ShardManager is supposed to do when it gets a request or response and a channel for the response.
// Ideally it would be generic with the type parameter being the response type
type Command struct {
	execute      func() (any, error)
	responseChan chan any
}

func newCommandWithChannel(execute func() (any, error), responseChannel chan any) *Command {
	return &Command{
		responseChan: responseChannel,
		execute:      execute,
	}
}

func newCommand(execute func() (any, error)) *Command {
	return newCommandWithChannel(execute, make(chan any, 1))
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

type waitingRoomEntry struct {
	counter             uint32
	confirmationChannel chan any
}

type cursor struct {
	status        CursorStatus
	lastPushed    *proto.EntryId
	lastConfirmed *proto.EntryId
}

type shardManager struct {
	shard              uint32
	epoch              uint64
	replicationFactor  uint32
	commitIndex        wal.EntryId
	headIndex          wal.EntryId
	followCursor       map[string]*cursor
	reconfigInProgress bool
	status             Status

	wal                 wal.Wal
	kv                  KeyValueStore
	commandChannel      chan *Command
	waitingRoom         map[wal.EntryId]waitingRoomEntry
	commitOffsetChannel chan wal.EntryId
	clientPool          common.ClientPool
	identityAddress     string
	closing             bool
	log                 zerolog.Logger
}

func NewShardManager(shard uint32, identityAddress string, pool common.ClientPool, w wal.Wal, kv KeyValueStore) (ShardManager, error) {
	sm := &shardManager{
		shard:              shard,
		epoch:              0,
		replicationFactor:  0,
		commitIndex:        wal.EntryId{},
		headIndex:          wal.EntryId{},
		followCursor:       make(map[string]*cursor),
		reconfigInProgress: false,
		status:             NotMember,

		wal:                 w,
		kv:                  kv,
		commandChannel:      make(chan *Command, 8),
		waitingRoom:         make(map[wal.EntryId]waitingRoomEntry),
		commitOffsetChannel: make(chan wal.EntryId, 1),
		clientPool:          pool,
		identityAddress:     identityAddress,
		closing:             false,
		log: log.With().
			Str("component", "shard-manager").
			Uint32("shard", shard).
			Logger(),
	}
	entryId, err := GetHighestEntryOfEpoch(sm.wal, MaxEpoch)
	if err != nil {
		return nil, err
	}
	sm.headIndex = entryId
	sm.log.Info().
		Uint32("replicationFactor", sm.replicationFactor).
		Msg("Start managing shard")
	go sm.run()

	return sm, nil
}

func enqueueCommandAndWaitForResponse[T any](s *shardManager, f func() (T, error)) T {
	response, _ := enqueueCommandAndWaitForResponseWithChannel[T](s, f, nil)
	return response
}
func enqueueCommandAndWaitForResponseWithChannel[T any](s *shardManager, f func() (T, error), responseChannel chan any) (T, error) {
	if responseChannel == nil {
		responseChannel = make(chan any, 1)
	}
	command := newCommandWithChannel(func() (any, error) {
		r, err := f()
		return r, err
	}, responseChannel)
	s.commandChannel <- command
	response := <-command.responseChan

	tResponse, ok := response.(T)
	if ok {
		return tResponse, nil
	} else {
		var zero T
		return zero, response.(error)
	}

}

func sendRequestAndProcessResponse[RESP any](s *shardManager, target string, send func(context.Context, proto.OxiaLogReplicationClient) (RESP, error), process func(RESP) error) error {
	rpc, err := s.clientPool.GetReplicationRpc(target)
	if err != nil {
		return err
	}
	go func() {
		ctx := context.Background()
		ctx = metadata.AppendToOutgoingContext(ctx,
			metadataShardId, fmt.Sprintf("%d", s.shard))
		resp, err2 := send(ctx, rpc)
		if err2 != nil {
			log.Error().Err(err2).Msg("Got error sending truncateRequest")
		}
		command := newCommand(func() (any, error) {
			err3 := process(resp)
			return nil, err3
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
		return status.Errorf(codes.FailedPrecondition, "Received message in the wrong state. In %+v, should be %+v.", s.status, expected)
	}
	return nil
}

// Fence enqueues a call to fenceSync and waits for the response
func (s *shardManager) Fence(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	response := enqueueCommandAndWaitForResponse[*proto.FenceResponse](s, func() (*proto.FenceResponse, error) {
		resp, err := s.fenceSync(req)
		return resp, err
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
func (s *shardManager) fenceSync(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	if err := s.checkEpochLaterIn(req); err != nil {
		return nil, err
	}
	s.epoch = req.GetEpoch()
	s.purgeWaitingRoom()
	oldStatus := s.status
	s.status = Fenced
	s.replicationFactor = 0
	s.followCursor = make(map[string]*cursor)
	s.waitingRoom = make(map[wal.EntryId]waitingRoomEntry)
	if err := s.CloseReaders(oldStatus); err != nil {
		return nil, err
	}
	s.reconfigInProgress = false
	return &proto.FenceResponse{
		Epoch:     s.epoch,
		HeadIndex: s.headIndex.ToProto(),
	}, nil
}

func (s *shardManager) BecomeLeader(req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	response := enqueueCommandAndWaitForResponse[*proto.BecomeLeaderResponse](s, func() (*proto.BecomeLeaderResponse, error) {
		resp, err := s.becomeLeaderSync(req)
		return resp, err
	})
	return response, nil
}

func (s *shardManager) needsTruncation(headIndex *proto.EntryId) bool {
	return headIndex.Epoch != s.headIndex.Epoch || headIndex.Offset > s.headIndex.Offset
}

func (s *shardManager) getCursor(headIndex *proto.EntryId) *cursor {
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

func (s *shardManager) getCursors(followers []*proto.BecomeLeaderRequest_FollowerEntry) map[string]*cursor {
	cursors := make(map[string]*cursor)
	for _, kv := range followers {
		cursors[kv.Key.InternalUrl] = s.getCursor(kv.Value)
	}
	return cursors
}

func (s *shardManager) sendTruncateRequest(target string, targetEpoch uint64) error {
	headIndex, err := GetHighestEntryOfEpoch(s.wal, targetEpoch)
	if err != nil {
		return err
	}
	send := func(ctx context.Context, rpc proto.OxiaLogReplicationClient) (*proto.TruncateResponse, error) {
		resp, err2 := rpc.Truncate(ctx, &proto.TruncateRequest{
			Epoch:     s.epoch,
			HeadIndex: headIndex.ToProto(),
		})
		return resp, err2
	}
	process := func(resp *proto.TruncateResponse) error {
		err2 := s.truncateResponseSync(target, resp)
		return err2
	}
	return sendRequestAndProcessResponse[*proto.TruncateResponse](s, target, send, process)
}

/*
truncateResponseSync: Leader handles a truncate response.

The leader now activates the follow cursor as the follower
log is now ready for replication.
*/
func (s *shardManager) truncateResponseSync(target string, resp *proto.TruncateResponse) error {
	if err := s.checkStatus(Leader); err != nil {
		return err
	}
	if err := s.checkEpochEqualIn(resp); err != nil {
		return err
	}
	s.followCursor[target] = &cursor{
		status:        Attached,
		lastPushed:    resp.HeadIndex,
		lastConfirmed: resp.HeadIndex,
	}
	s.sendEntriesToFollower(target, resp.HeadIndex)

	return nil
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
    the new leader will be informed of these followers, and it is
    possible that their head index is higher than the leader and
    therefore need truncating.
*/
func (s *shardManager) becomeLeaderSync(req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	if err := s.checkEpochEqualIn(req); err != nil {
		return nil, err
	}
	s.status = Leader
	s.replicationFactor = req.GetReplicationFactor()
	s.followCursor = s.getCursors(req.GetFollowerMaps())

	for k, v := range s.followCursor {
		if v.status == PendingTruncate {
			err := s.sendTruncateRequest(k, s.epoch)
			if err != nil {
				return nil, err
			}
		} else {
			s.sendEntriesToFollower(k, v.lastPushed)
		}
	}
	go s.processCommittedEntries(s.commitIndex)
	return &proto.BecomeLeaderResponse{Epoch: req.GetEpoch()}, nil
}
func (s *shardManager) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	response := enqueueCommandAndWaitForResponse[*proto.TruncateResponse](s, func() (*proto.TruncateResponse, error) {
		resp, err := s.truncateSync(req)
		return resp, err
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
func (s *shardManager) truncateSync(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	if err := s.checkStatus(Fenced); err != nil {
		return nil, err
	}
	if err := s.checkEpochEqualIn(req); err != nil {
		return nil, err
	}
	s.status = Follower
	s.epoch = req.Epoch
	headEntryId, err := s.wal.TruncateLog(wal.EntryIdFromProto(req.HeadIndex))
	if err != nil {
		return nil, err
	}
	s.headIndex = headEntryId
	s.followCursor = make(map[string]*cursor)

	return &proto.TruncateResponse{
		Epoch:     req.Epoch,
		HeadIndex: headEntryId.ToProto(),
	}, nil
}

func (s *shardManager) AddFollower(req *proto.AddFollowerRequest) (*proto.CoordinationEmpty, error) {
	response := enqueueCommandAndWaitForResponse[*proto.CoordinationEmpty](s, func() (*proto.CoordinationEmpty, error) {
		resp, err := s.addFollowerSync(req)
		return resp, err
	})
	return response, nil
}

/*
Leader handles an Add Follower request

The leader creates a cursor and will send a truncate
request to the follower if their log might need
truncating first.
*/
func (s *shardManager) addFollowerSync(req *proto.AddFollowerRequest) (*proto.CoordinationEmpty, error) {
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

	return &proto.CoordinationEmpty{}, nil
}

func (s *shardManager) Write(op *proto.PutOp) (*proto.Stat, error) {
	responseChannel := make(chan any, 1)
	response, err := enqueueCommandAndWaitForResponseWithChannel[proto.Stat](s, func() (proto.Stat, error) {
		err2 := s.writeSync(op, responseChannel)
		return proto.Stat{}, err2
	}, responseChannel)
	return &response, err
}

func serializeOp(op *proto.PutOp) ([]byte, error) {
	val, err := json.Marshal(op)
	return val, err
}

func deserializeOp(encoding []byte) (*proto.PutOp, error) {
	data := &proto.PutOp{}
	err := json.Unmarshal(encoding, data)
	return data, err
}

/*
writeSync A client writes an entry to the leader

	A client writes a value from Values to a leader node
	if that value has not previously been written. The leader adds
	the entry to its log, updates its head_index.
*/
func (s *shardManager) writeSync(op *proto.PutOp, responseChannel chan any) error {
	s.log.Debug().
		Interface("op", op).
		Msg("Put operation")

	if err := s.checkStatus(Leader); err != nil {
		return err
	}
	entryId := &proto.EntryId{
		Epoch:  s.epoch,
		Offset: s.headIndex.Offset + 1,
	}
	value, err := serializeOp(op)
	if err != nil {
		return err
	}
	logEntry := &proto.LogEntry{
		EntryId:   entryId,
		Value:     value,
		Timestamp: uint64(time.Now().Nanosecond()),
	}
	// Note: the version in the KV store may be updated by the time this entry is applied
	err = s.wal.Append(logEntry)
	if err != nil {
		return err
	}
	s.headIndex = wal.EntryIdFromProto(entryId)
	s.waitingRoom[s.headIndex] = waitingRoomEntry{
		counter:             0,
		confirmationChannel: responseChannel,
	}
	return nil

}

func (s *shardManager) sendEntriesToFollower(target string, lastPushedEntry *proto.EntryId) {
	err := sendRequestAndProcessResponse[any](s, target, func(ctx context.Context, client proto.OxiaLogReplicationClient) (any, error) {
		addEntries, err := client.AddEntries(ctx)
		if err != nil {
			return nil, err
		}
		r, err := s.wal.NewReader(wal.EntryIdFromProto(lastPushedEntry))
		if err != nil {
			return nil, err
		}

		go func() {
			defer func() {
				err = r.Close()
				if err != nil {
					s.log.Err(err).Msg("Error closing reader used for replication")
				}
			}()
			for s.status == Leader {
				entry, err := r.ReadNext()
				if err == wal.ErrorReaderClosed {
					s.log.Info().Msg("Stopped reading entries for replication")
					return
				} else if err != nil {
					s.log.Err(err).Msg("Error reading entries for replication")
					return
				}
				err = addEntries.Send(&proto.AddEntryRequest{
					Epoch:       s.epoch,
					Entry:       entry,
					CommitIndex: s.commitIndex.ToProto(),
				})
				if err != nil {
					log.Error().Err(err).Msgf("Error replicating message to %s", target)
					return
				}
				// TODO s.Lock()
				s.followCursor[target].lastPushed = entry.EntryId
				// TODO s.UnLock()
			}
		}()

		go func() {
			for {
				addEntryResponse, err2 := addEntries.Recv()
				if err2 != nil {
					log.Error().Err(err2).Msg("Error while waiting for ack")
					return
				}
				command := newCommand(func() (any, error) {
					err3 := s.addEntryResponseSync(target, addEntryResponse)
					return nil, err3
				})
				s.commandChannel <- command
			}
		}()
		return nil, nil
	}, func(any) error {
		return nil
	})
	if err != nil {
		log.Error().Err(err).Msgf("Error sending message to '%s'", target)
	}
}

/*
A leader node handles an add entry response

	The leader updates the follow cursor last_confirmed
	entry id, it also may advance the commit index.

	An entry is committed when all the following
	has occurred:
	- it has reached majority quorum
	- the entire log prefix has reached majority quorum
	- the entry epoch matches the current epoch

	Key points:
	- Entries of prior epochs cannot be committed directly by
	  themselves, only entries of the current epoch can be
	  committed. Committing an entry of the current epoch
	  also implicitly includes all prior entries.
	  To find a counterexample where loss of confirmed writes
	  occurs when committing entries of prior epochs directly,
	  comment out the condition 'entry_id.epoch = nstate.epoch'
	  below. Also see the Raft paper.
*/
func (s *shardManager) addEntryResponseSync(follower string, response *proto.AddEntryResponse) error {
	if err := s.checkStatus(Leader); err != nil {
		return err
	}
	if err := s.checkEpochEqualIn(response); err != nil {
		return nil
	}
	// TODO check if 'IsEarliestReceivableEntryMessage'
	if !response.InvalidEpoch && s.followCursor[follower].status == Attached {
		s.followCursor[follower].lastConfirmed = response.EntryId
		// TODO LogPrefixAtQuorum
		waitingEntry, ok := s.waitingRoom[wal.EntryIdFromProto(response.EntryId)]
		if ok {
			// If the entry is not there, it has already been confirmed
			waitingEntry.counter++
			if waitingEntry.counter >= s.replicationFactor/2 {
				s.commitIndex = wal.EntryIdFromProto(response.EntryId)
				s.commitOffsetChannel <- s.commitIndex
			}
		}
	} else {
		log.Error().Msgf("Entry rejected by '%s'", follower)
	}
	return nil
}

func (s *shardManager) processCommittedEntries(minExclusive wal.EntryId) {
	reader, err := s.wal.NewReader(minExclusive)
	if err != nil {
		s.log.Err(err).Msg("Error opening reader used for committed entries")
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			s.log.Err(err).Msg("Error closing reader used for committed entries")
		}
	}()
	limit := s.commitIndex
	for {
		entry, err2 := reader.ReadNext()
		for err2 == nil && wal.EntryIdFromProto(entry.EntryId).LessOrEqual(limit) {
			op, err := deserializeOp(entry.GetValue())
			if err != nil {
				s.log.Err(err).Msg("Error deserializing committed entry for processing")
				return
			}
			// TODO synchronization
			stat, err := s.kv.Apply(op, entry.Timestamp)
			if err != nil {
				s.log.Err(err).Msg("Error applying committed entry")
				return
			}
			waitingEntry := s.waitingRoom[wal.EntryIdFromProto(entry.EntryId)]
			waitingEntry.confirmationChannel <- stat
			delete(s.waitingRoom, wal.EntryIdFromProto(entry.EntryId))
			// TODO notify watchers
			entry, err2 = reader.ReadNext()
		}
		if err2 == wal.ErrorReaderClosed {
			s.log.Info().Msg("Stopped reading committed entries for processing")
			return
		} else if err2 != nil {
			s.log.Err(err2).Msg("Error reading committed entry for processing")
			return
		}
		newLimit, more := <-s.commitOffsetChannel
		if more {
			limit = newLimit
		} else {
			break
		}
	}
}

func (s *shardManager) AddEntries(srv proto.OxiaLogReplication_AddEntriesServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return err
		}
		response := enqueueCommandAndWaitForResponse[*proto.AddEntryResponse](s, func() (*proto.AddEntryResponse, error) {
			if resp, err := s.addEntrySync(req); err != nil {
				return nil, err
			} else {
				return resp, nil
			}
		})
		err = srv.Send(response)
		if err != nil {
			return err
		}
	}
}

func (s *shardManager) addEntrySync(req *proto.AddEntryRequest) (*proto.AddEntryResponse, error) {
	if req.GetEpoch() <= s.epoch {
		/*
		 A follower node rejects an entry from the leader.


		  If the leader has a lower epoch than the follower then the
		  follower must reject it with an INVALID_EPOCH response.

		  Key points:
		  - The epoch of the response should be the epoch of the
		    request so that the leader will not ignore the response.
		*/
		return &proto.AddEntryResponse{
			Epoch:        req.Epoch,
			EntryId:      nil,
			InvalidEpoch: true,
		}, nil
	}
	if s.status != Follower && s.status != Fenced {
		return nil, status.Errorf(codes.FailedPrecondition, "AddEntry request when status = %+v", s.status)
	}

	/*
	  A follower node confirms an entry to the leader

	  The follower adds the entry to its log, sets the head index
	  and updates its commit index with the commit index of
	  the request.
	*/
	s.status = Follower
	s.epoch = req.Epoch
	err := s.wal.Append(req.GetEntry())
	if err != nil {
		return nil, err
	}
	s.headIndex = wal.EntryIdFromProto(req.Entry.EntryId)
	oldCommitIndex := s.commitIndex
	s.commitIndex = wal.EntryIdFromProto(req.CommitIndex)
	maxInclusive := s.commitIndex
	// TODO maybe a long lived go routine instead of a go routine per request
	go func() {
		reader, err := s.wal.NewReader(oldCommitIndex)
		if err != nil {
			s.log.Err(err).Msg("Error opening reader used for applying committed entries")
			return
		}
		defer func() {
			err := reader.Close()
			if err != nil {
				s.log.Err(err).Msg("Error closing reader used for applying committed entries")
			}
		}()
		entry, err2 := reader.ReadNext()
		for err2 == nil && wal.EntryIdFromProto(entry.EntryId).LessOrEqual(maxInclusive) {

			op, err := deserializeOp(entry.GetValue())
			if err != nil {
				s.log.Err(err).Msg("Error deserializing committed entry")
				return
			}
			_, err = s.kv.Apply(op, entry.Timestamp)
			if err != nil {
				s.log.Err(err).Msg("Error applying committed entry")
				return
			}
			entry, err2 = reader.ReadNext()
		}
		if err2 == wal.ErrorReaderClosed {
			s.log.Info().Msg("Stopped reading committed entries")
			return
		} else if err2 != nil {
			s.log.Err(err2).Msg("Error reading committed entry")
			return
		}
	}()
	return &proto.AddEntryResponse{
		Epoch:        s.epoch,
		EntryId:      req.Entry.EntryId,
		InvalidEpoch: false,
	}, nil

}

func (s *shardManager) PrepareReconfig(req *proto.PrepareReconfigRequest) (*proto.PrepareReconfigResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PrepareReconfig not implemented")
}
func (s *shardManager) Snapshot(req *proto.SnapshotRequest) (*proto.SnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Snapshot not implemented")
}
func (s *shardManager) CommitReconfig(req *proto.CommitReconfigRequest) (*proto.CommitReconfigResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CommitReconfig not implemented")
}

// run takes commands from the queue and executes them serially
func (s *shardManager) run() {
	for !s.closing {
		command := <-s.commandChannel
		response, err := command.execute()
		if response != nil {
			command.responseChan <- response
		}
		if err != nil {
			log.Error().Err(err).Msg("Error executing command.")
		}
		if s.closing || err != nil {
			log.Info().Msg("Shard manager closes.")
			break
		}
	}
}

func (s *shardManager) Close() error {
	s.log.Info().Msg("Closing shard manager")
	s.commandChannel <- newCommand(func() (any, error) {
		s.purgeWaitingRoom()
		s.status = NotMember
		return nil, nil
	})

	return s.wal.Close()
}

func (s *shardManager) purgeWaitingRoom() {
	for _, v := range s.waitingRoom {
		v.confirmationChannel <- status.Errorf(codes.Aborted, "oxia: server shutting down")
	}
	s.waitingRoom = make(map[wal.EntryId]waitingRoomEntry)
}

func (s *shardManager) CloseReaders(previousStatus Status) error {
	if previousStatus == Leader {
		close(s.commitOffsetChannel)
	}
	return nil
}
