package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
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

	// Epoch The current epoch of the leader
	Epoch() uint64

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
	epoch             uint64
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
		status:            NotMember,
		shardId:           shardId,
		epoch:             0,
		replicationFactor: 0,
		quorumAckTracker:  nil,
		rpcClient:         rpcClient,
		followers:         make(map[string]FollowerCursor),

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

	return lc, nil
}

func (lc *leaderController) Status() Status {
	lc.Lock()
	defer lc.Unlock()
	return lc.status
}

func (lc *leaderController) Epoch() uint64 {
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

	if err := checkEpochLaterIn(req, lc.epoch); err != nil {
		return nil, err
	}

	lc.epoch = req.GetEpoch()
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

	return &proto.FenceResponse{
		Epoch:     lc.epoch,
		HeadIndex: lc.wal.LastEntry().ToProto(),
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

	lc.log.Info().
		Interface("request", req).
		Msg("BecomeLeader")

	if err := checkEpochEqualIn(req, lc.epoch); err != nil {
		return nil, err
	}

	lc.status = Leader
	lc.replicationFactor = req.GetReplicationFactor()
	lc.followers = make(map[string]FollowerCursor)

	leaderHeadIndex := lc.wal.LastEntry()

	lc.quorumAckTracker = NewQuorumAckTracker(req.GetReplicationFactor(), leaderHeadIndex)

	for follower, followerHeadIndex := range req.FollowerMaps {
		if needsTruncation(leaderHeadIndex.ToProto(), followerHeadIndex) {
			newHeadIndex, err := lc.truncateFollower(follower, lc.epoch)
			if err != nil {
				lc.log.Error().Err(err).
					Str("follower", follower).
					Uint64("epoch", lc.epoch).
					Msg("Failed to truncate follower")
				return nil, err
			}

			followerHeadIndex = newHeadIndex
		}

		cursor, err := NewFollowerCursor(follower, lc.epoch, lc.shardId, lc.rpcClient, lc.quorumAckTracker, lc.wal,
			wal.EntryIdFromProto(followerHeadIndex))
		if err != nil {
			lc.log.Error().Err(err).
				Str("follower", follower).
				Uint64("epoch", lc.epoch).
				Msg("Failed to create follower cursor")
			return nil, err
		}

		lc.followers[follower] = cursor
	}
	return &proto.BecomeLeaderResponse{Epoch: req.GetEpoch()}, nil
}

func needsTruncation(leaderHeadIndex *proto.EntryId, followerHeadIndex *proto.EntryId) bool {
	return followerHeadIndex.Epoch != leaderHeadIndex.Epoch ||
		followerHeadIndex.Offset > leaderHeadIndex.Offset
}

func (lc *leaderController) truncateFollower(follower string, targetEpoch uint64) (*proto.EntryId, error) {
	headIndex, err := GetHighestEntryOfEpoch(lc.wal, targetEpoch)
	if err != nil {
		return nil, err
	}

	tr, err := lc.rpcClient.Truncate(follower, &proto.TruncateRequest{
		Epoch:     lc.epoch,
		HeadIndex: headIndex.ToProto(),
	})

	if err != nil {
		return nil, err
	} else {
		lc.log.Info().
			Uint64("epoch", lc.epoch).
			Str("follower", follower).
			Interface("head-index", tr.HeadIndex).
			Msg("Truncated follower")

		return tr.HeadIndex, nil
	}
}

func (lc *leaderController) Read(request *proto.ReadRequest) (*proto.ReadResponse, error) {
	lc.log.Debug().
		Interface("read", request).
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
		Interface("request", request).
		Msg("Write operation")

	var entryId *proto.EntryId

	{
		lc.Lock()
		defer lc.Unlock()

		if err := checkStatus(Leader, lc.status); err != nil {
			return nil, err
		}
		entryId = &proto.EntryId{
			Epoch:  lc.epoch,
			Offset: lc.quorumAckTracker.HeadIndex().Offset + 1,
		}
		value, err := pb.Marshal(request)
		if err != nil {
			return nil, err
		}
		logEntry := &proto.LogEntry{
			EntryId: entryId,
			Value:   value,
		}

		err = lc.wal.Append(logEntry)
		if err != nil {
			return nil, err
		}
	}

	id := wal.EntryIdFromProto(entryId)
	lc.quorumAckTracker.AdvanceHeadIndex(id)

	return lc.quorumAckTracker.WaitForCommitIndex(id, func() (*proto.WriteResponse, error) {
		return lc.db.ProcessWrite(request, id.ToProto())
	})
}

func (lc *leaderController) Close() error {
	lc.Lock()
	defer lc.Unlock()

	lc.log.Info().Msg("Closing leader controller")

	lc.status = NotMember
	var err error
	for _, follower := range lc.followers {
		err = multierr.Append(err, follower.Close())
	}

	if lc.quorumAckTracker != nil {
		err = multierr.Append(err, lc.quorumAckTracker.Close())
	}

	err = multierr.Combine(err,
		lc.wal.Close(),
		lc.db.Close(),
	)
	return err
}
