package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"
	"io"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
)

type LeaderController interface {
	io.Closer

	Write(write *proto.WriteRequest) (*proto.WriteResponse, error)
	Read(read *proto.ReadRequest) (*proto.ReadResponse, error)

	// Fence Handle fence request
	Fence(req *proto.FenceRequest) (*proto.FenceResponse, error)

	// BecomeLeader Handles BecomeLeaderRequest from coordinator and prepares to be leader for the shard
	BecomeLeader(*proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)

	AddFollower(request *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)

	// Epoch The current epoch of the leader
	Epoch() int64

	// Status The Status of the leader
	Status() Status
}

type Status int16

const (
	NotMember Status = iota
	Leader
	Follower
	Fenced
)

type leaderController struct {
	sync.Mutex

	shardId           uint32
	status            Status
	epoch             int64
	replicationFactor uint32
	quorumAckTracker  QuorumAckTracker
	followers         map[string]FollowerCursor

	wal       wal.Wal
	db        kv.DB
	rpcClient ReplicationRpcProvider
	log       zerolog.Logger
}

func NewLeaderController(shardId uint32, rpcClient ReplicationRpcProvider, walFactory wal.WalFactory, kvFactory kv.KVFactory) (LeaderController, error) {
	lc := &leaderController{
		status:           NotMember,
		shardId:          shardId,
		quorumAckTracker: nil,
		rpcClient:        rpcClient,
		followers:        make(map[string]FollowerCursor),

		log: log.With().
			Str("component", "leader-controller").
			Uint32("shard", shardId).
			Logger(),
	}

	var err error
	if lc.wal, err = walFactory.NewWal(shardId); err != nil {
		return nil, err
	}

	if lc.db, err = kv.NewDB(shardId, kvFactory); err != nil {
		return nil, err
	}

	if lc.epoch, err = lc.db.ReadEpoch(); err != nil {
		return nil, err
	}

	lc.log = lc.log.With().Int64("epoch", lc.epoch).Logger()
	lc.log.Info().
		Msg("Created leader controller")
	return lc, nil
}

func (lc *leaderController) Status() Status {
	lc.Lock()
	defer lc.Unlock()
	return lc.status
}

func (lc *leaderController) Epoch() int64 {
	lc.Lock()
	defer lc.Unlock()
	return lc.epoch
}

// Fence
//
// # Node handles a fence request
//
// A node receives a fencing request, fences itself and responds
// with its head index.
//
// When a node is fenced it cannot:
//   - accept any writes from a client.
//   - accept add entry addEntryRequests from a leader.
//   - send any entries to followers if it was a leader.
//
// Any existing follow cursors are destroyed as is any state
// regarding reconfigurations.
func (lc *leaderController) Fence(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Epoch <= lc.epoch {
		return nil, status.Errorf(CodeInvalidEpoch, "invalid epoch - current epoch %d - request epoch %d", lc.epoch, req.Epoch)
	}

	if err := lc.db.UpdateEpoch(req.GetEpoch()); err != nil {
		return nil, err
	}

	lc.epoch = req.GetEpoch()
	lc.log = lc.log.With().Int64("epoch", lc.epoch).Logger()
	lc.status = Fenced
	lc.replicationFactor = 0

	if lc.quorumAckTracker != nil {
		if err := lc.quorumAckTracker.Close(); err != nil {
			return nil, err
		}
		lc.quorumAckTracker = nil
	}

	for _, follower := range lc.followers {
		if err := follower.Close(); err != nil {
			return nil, err
		}
	}

	lc.followers = nil
	headIndex, err := getLastEntryIdInWal(lc.wal)
	if err != nil {
		return nil, err
	}

	lc.log.Info().
		Interface("last-entry", headIndex).
		Msg("Fenced leader")

	return &proto.FenceResponse{
		HeadIndex: headIndex,
	}, nil
}

// BecomeLeader : Node handles a Become Leader request
//
// The node inspects the head index of each follower and
// compares it to its own head index, and then either:
//   - Attaches a follow cursor for the follower the head indexes
//     have the same epoch, but the follower offset is lower or equal.
//   - Sends a truncate request to the follower if its head
//     index epoch does not match the leader's head index epoch or has
//     a higher offset.
//     The leader finds the highest entry id in its log prefix (of the
//     follower head index) and tells the follower to truncate its log
//     to that entry.
//
// Key points:
//   - The election only requires a majority to complete and so the
//     Become Leader request will likely only contain a majority,
//     not all the nodes.
//   - No followers in the Become Leader message "follower map" will
//     have a higher head index than the leader (as the leader was
//     chosen because it had the highest head index of the majority
//     that responded to the fencing addEntryRequests first). But as the leader
//     receives more fencing addEntryResponses from the remaining minority,
//     the new leader will be informed of these followers, and it is
//     possible that their head index is higher than the leader and
//     therefore need truncating.
func (lc *leaderController) BecomeLeader(req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Epoch != lc.epoch {
		return nil, status.Errorf(CodeInvalidEpoch, "invalid epoch - current epoch %d - request epoch %d", lc.epoch, req.Epoch)
	}

	lc.status = Leader
	lc.replicationFactor = req.GetReplicationFactor()
	lc.followers = make(map[string]FollowerCursor)

	leaderHeadIndex, err := getLastEntryIdInWal(lc.wal)
	if err != nil {
		return nil, err
	}

	leaderCommitIndex, err := lc.db.ReadCommitIndex()
	if err != nil {
		return nil, err
	}

	lc.quorumAckTracker = NewQuorumAckTracker(req.GetReplicationFactor(), leaderHeadIndex.Offset, leaderCommitIndex)

	for follower, followerHeadIndex := range req.FollowerMaps {
		if err := lc.addFollower(leaderHeadIndex, follower, followerHeadIndex); err != nil {
			return nil, err
		}
	}

	// We must wait until all the entries in the leader WAL are fully
	// committed in the quorum, to avoid missing any entries in the DB
	// by the moment we make the leader controller accepting new write/read
	// requests
	if _, err = lc.quorumAckTracker.WaitForCommitIndex(leaderHeadIndex.Offset, nil); err != nil {
		return nil, err
	}

	if err = lc.applyAllEntriesIntoDB(); err != nil {
		return nil, err
	}

	lc.log.Info().
		Int64("epoch", lc.epoch).
		Int64("head-index", leaderHeadIndex.Offset).
		Msg("Started leading the shard")
	return &proto.BecomeLeaderResponse{}, nil
}

func (lc *leaderController) AddFollower(req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Epoch != lc.epoch {
		return nil, status.Errorf(CodeInvalidEpoch, "invalid epoch - current epoch %d - request epoch %d", lc.epoch, req.Epoch)
	}

	if lc.status != Leader {
		return nil, status.Error(CodeInvalidStatus, "Node is not leader")
	}

	if _, followerAlreadyPresent := lc.followers[req.FollowerName]; followerAlreadyPresent {
		return nil, errors.Errorf("follower %s is already present", req.FollowerName)
	}

	if len(lc.followers) == int(lc.replicationFactor)-1 {
		return nil, errors.New("all followers are already attached")
	}

	leaderHeadIndex, err := getLastEntryIdInWal(lc.wal)
	if err != nil {
		return nil, err
	}

	if err := lc.addFollower(leaderHeadIndex, req.FollowerName, req.FollowerHeadIndex); err != nil {
		return nil, err
	}

	return &proto.AddFollowerResponse{}, nil
}

func (lc *leaderController) addFollower(leaderHeadIndex *proto.EntryId, follower string, followerHeadIndex *proto.EntryId) error {
	if needsTruncation(leaderHeadIndex, followerHeadIndex) {
		newHeadIndex, err := lc.truncateFollower(follower, lc.epoch)
		if err != nil {
			lc.log.Error().Err(err).
				Str("follower", follower).
				Int64("epoch", lc.epoch).
				Msg("Failed to truncate follower")
			return err
		}

		followerHeadIndex = newHeadIndex
	}

	cursor, err := NewFollowerCursor(follower, lc.epoch, lc.shardId, lc.rpcClient, lc.quorumAckTracker, lc.wal,
		followerHeadIndex.Offset)
	if err != nil {
		lc.log.Error().Err(err).
			Str("follower", follower).
			Int64("epoch", lc.epoch).
			Msg("Failed to create follower cursor")
		return err
	}

	lc.log.Info().
		Int64("epoch", lc.epoch).
		Interface("head-index", leaderHeadIndex).
		Str("follower", follower).
		Interface("follower-head-index", followerHeadIndex).
		Msg("Added follower")
	lc.followers[follower] = cursor
	return nil
}

func (lc *leaderController) applyAllEntriesIntoDB() error {
	dbCommitIndex, err := lc.db.ReadCommitIndex()
	if err != nil {
		return err
	}

	lc.log.Info().
		Int64("commit-index", dbCommitIndex).
		Int64("head-index", lc.quorumAckTracker.HeadIndex()).
		Msg("Applying all pending entries to database")

	r, err := lc.wal.NewReader(dbCommitIndex)
	if err != nil {
		return err
	}
	for r.HasNext() {
		entry, err := r.ReadNext()
		if err != nil {
			return err
		}

		writeRequest := &proto.WriteRequest{}
		if err = pb.Unmarshal(entry.Value, writeRequest); err != nil {
			return err
		}

		if _, err = lc.db.ProcessWrite(writeRequest, entry.Offset); err != nil {
			return err
		}
	}

	return nil
}

func needsTruncation(leaderHeadIndex *proto.EntryId, followerHeadIndex *proto.EntryId) bool {
	return followerHeadIndex.Epoch != leaderHeadIndex.Epoch ||
		followerHeadIndex.Offset > leaderHeadIndex.Offset
}

func (lc *leaderController) truncateFollower(follower string, targetEpoch int64) (*proto.EntryId, error) {
	headIndex, err := GetHighestEntryOfEpoch(lc.wal, targetEpoch)
	if err != nil {
		return nil, err
	}

	tr, err := lc.rpcClient.Truncate(follower, &proto.TruncateRequest{
		Epoch:     lc.epoch,
		HeadIndex: headIndex,
	})

	if err != nil {
		return nil, err
	} else {
		lc.log.Info().
			Int64("epoch", lc.epoch).
			Str("follower", follower).
			Interface("head-index", tr.HeadIndex).
			Msg("Truncated follower")

		return tr.HeadIndex, nil
	}
}

func (lc *leaderController) Read(request *proto.ReadRequest) (*proto.ReadResponse, error) {
	lc.log.Debug().
		Interface("req", request).
		Msg("Received read request")

	{
		lc.Lock()
		defer lc.Unlock()
		if err := checkStatus(Leader, lc.status); err != nil {
			return nil, err
		}
	}

	return lc.db.ProcessRead(request)
}

// Write
// A client sends a batch of entries to the leader
//
// A client writes a value from Values to a leader node
// if that value has not previously been written. The leader adds
// the entry to its log, updates its head_index.
func (lc *leaderController) Write(request *proto.WriteRequest) (*proto.WriteResponse, error) {
	lc.log.Debug().
		Interface("req", request).
		Msg("Write operation")

	var newOffset int64

	{
		lc.Lock()
		defer lc.Unlock()

		if err := checkStatus(Leader, lc.status); err != nil {
			return nil, err
		}

		newOffset = lc.quorumAckTracker.HeadIndex() + 1
		value, err := pb.Marshal(request)
		if err != nil {
			return nil, err
		}
		logEntry := &proto.LogEntry{
			Epoch:  lc.epoch,
			Offset: newOffset,
			Value:  value,
		}

		err = lc.wal.Append(logEntry)
		if err != nil {
			return nil, err
		}
	}

	lc.quorumAckTracker.AdvanceHeadIndex(newOffset)

	return lc.quorumAckTracker.WaitForCommitIndex(newOffset, func() (*proto.WriteResponse, error) {
		return lc.db.ProcessWrite(request, newOffset)
	})
}

func (lc *leaderController) Close() error {
	lc.Lock()
	defer lc.Unlock()

	lc.log.Info().Msg("Closing leader controller")

	lc.status = NotMember

	var err error
	if lc.quorumAckTracker != nil {
		err = multierr.Append(err, lc.quorumAckTracker.Close())
	}

	for _, follower := range lc.followers {
		err = multierr.Append(err, follower.Close())
	}

	err = multierr.Combine(err,
		lc.wal.Close(),
		lc.db.Close(),
	)
	return err
}

func getLastEntryIdInWal(wal wal.Wal) (*proto.EntryId, error) {
	reader, err := wal.NewReverseReader()
	if err != nil {
		return nil, err
	}

	if !reader.HasNext() {
		return InvalidEntryId, nil
	}

	entry, err := reader.ReadNext()
	if err != nil {
		return nil, err
	}
	return &proto.EntryId{Epoch: entry.Epoch, Offset: entry.Offset}, nil
}
