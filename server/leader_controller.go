// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"
	"io"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
	"time"
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

	GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error

	GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error)

	// Epoch The current epoch of the leader
	Epoch() int64

	// Status The Status of the leader
	Status() proto.ServingStatus

	CreateSession(*proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(sessionId int64, stream proto.OxiaClient_KeepAliveServer) error
	CloseSession(*proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
}

type leaderController struct {
	sync.Mutex

	shardId           uint32
	status            proto.ServingStatus
	epoch             int64
	replicationFactor uint32
	quorumAckTracker  QuorumAckTracker
	followers         map[string]FollowerCursor

	// This represents the last entry in the WAL at the time this node
	// became leader. It's used in the logic for deciding where to
	// truncate the followers.
	leaderElectionHeadEntryId *proto.EntryId

	ctx            context.Context
	cancel         context.CancelFunc
	wal            wal.Wal
	walTrimmer     wal.Trimmer
	db             kv.DB
	rpcClient      ReplicationRpcProvider
	sessionManager SessionManager
	log            zerolog.Logger

	writeLatencyHisto       metrics.LatencyHistogram
	headOffsetGauge         metrics.Gauge
	commitOffsetGauge       metrics.Gauge
	followerAckOffsetGauges map[string]metrics.Gauge
}

func NewLeaderController(config Config, shardId uint32, rpcClient ReplicationRpcProvider, walFactory wal.WalFactory, kvFactory kv.KVFactory) (LeaderController, error) {
	labels := metrics.LabelsForShard(shardId)
	lc := &leaderController{
		status:           proto.ServingStatus_NotMember,
		shardId:          shardId,
		quorumAckTracker: nil,
		rpcClient:        rpcClient,
		followers:        make(map[string]FollowerCursor),

		log: log.With().
			Str("component", "leader-controller").
			Uint32("shard", shardId).
			Logger(),
		writeLatencyHisto: metrics.NewLatencyHistogram("oxia_server_leader_write_latency",
			"Latency for write operations in the leader", labels),
		followerAckOffsetGauges: map[string]metrics.Gauge{},
	}

	lc.headOffsetGauge = metrics.NewGauge("oxia_server_leader_head_offset", "The current head offset", "offset", labels, func() int64 {
		lc.Lock()
		defer lc.Unlock()
		if lc.quorumAckTracker != nil {
			return lc.quorumAckTracker.HeadOffset()
		}

		return -1
	})
	lc.commitOffsetGauge = metrics.NewGauge("oxia_server_leader_commit_offset", "The current commit offset", "offset", labels, func() int64 {
		lc.Lock()
		defer lc.Unlock()
		if lc.quorumAckTracker != nil {
			return lc.quorumAckTracker.CommitOffset()
		}

		return -1
	})

	lc.sessionManager = NewSessionManager(shardId, lc)

	lc.ctx, lc.cancel = context.WithCancel(context.Background())

	var err error
	if lc.wal, err = walFactory.NewWal(shardId); err != nil {
		return nil, err
	}

	lc.walTrimmer = wal.NewTrimmer(shardId, lc.wal, config.WalRetentionTime, wal.DefaultCheckInterval, common.SystemClock)

	if lc.db, err = kv.NewDB(shardId, kvFactory, config.NotificationsRetentionTime, common.SystemClock); err != nil {
		return nil, err
	}

	if lc.epoch, err = lc.db.ReadEpoch(); err != nil {
		return nil, err
	}

	if lc.epoch != wal.InvalidEpoch {
		lc.status = proto.ServingStatus_Fenced
	}

	lc.log = lc.log.With().Int64("epoch", lc.epoch).Logger()
	lc.log.Info().
		Msg("Created leader controller")
	return lc, nil
}

func (lc *leaderController) Status() proto.ServingStatus {
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
// with its head offset.
//
// When a node is fenced it cannot:
//   - accept any writes from a client.
//   - accept add entry append from a leader.
//   - send any entries to followers if it was a leader.
//
// Any existing follow cursors are destroyed as is any state
// regarding reconfigurations.
func (lc *leaderController) Fence(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Epoch < lc.epoch {
		return nil, common.ErrorInvalidEpoch
	} else if req.Epoch == lc.epoch && lc.status != proto.ServingStatus_Fenced {
		// It's OK to receive a duplicate Fence request, for the same epoch, as long as we haven't moved
		// out of the Fenced state for that epoch
		lc.log.Warn().
			Int64("follower-epoch", lc.epoch).
			Int64("fence-epoch", req.Epoch).
			Interface("status", lc.status).
			Msg("Failed to fence with same epoch in invalid state")
		return nil, common.ErrorInvalidStatus
	}

	if err := lc.db.UpdateEpoch(req.GetEpoch()); err != nil {
		return nil, err
	}

	lc.epoch = req.GetEpoch()
	lc.log = lc.log.With().Int64("epoch", lc.epoch).Logger()
	lc.status = proto.ServingStatus_Fenced
	lc.replicationFactor = 0

	lc.headOffsetGauge.Unregister()
	lc.commitOffsetGauge.Unregister()

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

	for _, g := range lc.followerAckOffsetGauges {
		g.Unregister()
	}

	lc.followers = nil
	headEntryId, err := getLastEntryIdInWal(lc.wal)
	if err != nil {
		return nil, err
	}

	err = lc.sessionManager.Close()
	if err != nil {
		return nil, err
	}

	lc.log.Info().
		Interface("last-entry", headEntryId).
		Msg("Fenced leader")

	return &proto.FenceResponse{
		HeadEntryId: headEntryId,
	}, nil
}

// BecomeLeader : Node handles a Become Leader request
//
// The node inspects the head offset of each follower and
// compares it to its own head offset, and then either:
//   - Attaches a follow cursor for the follower the head entry ids
//     have the same epoch, but the follower offset is lower or equal.
//   - Sends a truncate request to the follower if its head
//     entry epoch does not match the leader's head entry epoch or has
//     a higher offset.
//     The leader finds the highest entry id in its log prefix (of the
//     follower head entry) and tells the follower to truncate its log
//     to that entry.
//
// Key points:
//   - The election only requires a majority to complete and so the
//     Become Leader request will likely only contain a majority,
//     not all the nodes.
//   - No followers in the Become Leader message "follower map" will
//     have a higher head offset than the leader (as the leader was
//     chosen because it had the highest head entry of the majority
//     that responded to the fencing append first). But as the leader
//     receives more fencing acks from the remaining minority,
//     the new leader will be informed of these followers, and it is
//     possible that their head entry id is higher than the leader and
//     therefore need truncating.
func (lc *leaderController) BecomeLeader(req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if lc.status != proto.ServingStatus_Fenced {
		return nil, common.ErrorInvalidStatus
	}

	if req.Epoch != lc.epoch {
		return nil, common.ErrorInvalidEpoch
	}

	lc.status = proto.ServingStatus_Leader
	lc.replicationFactor = req.GetReplicationFactor()
	lc.followers = make(map[string]FollowerCursor)

	var err error
	lc.leaderElectionHeadEntryId, err = getLastEntryIdInWal(lc.wal)
	if err != nil {
		return nil, err
	}

	leaderCommitOffset, err := lc.db.ReadCommitOffset()
	if err != nil {
		return nil, err
	}

	lc.quorumAckTracker = NewQuorumAckTracker(req.GetReplicationFactor(), lc.leaderElectionHeadEntryId.Offset, leaderCommitOffset)

	for follower, followerHeadEntryId := range req.FollowerMaps {
		if err := lc.addFollower(follower, followerHeadEntryId); err != nil {
			return nil, err
		}
	}

	// We must wait until all the entries in the leader WAL are fully
	// committed in the quorum, to avoid missing any entries in the DB
	// by the moment we make the leader controller accepting new write/read
	// requests
	if _, err = lc.quorumAckTracker.WaitForCommitOffset(lc.leaderElectionHeadEntryId.Offset, nil); err != nil {
		return nil, err
	}

	if err = lc.applyAllEntriesIntoDB(); err != nil {
		return nil, err
	}

	lc.log.Info().
		Int64("epoch", lc.epoch).
		Int64("head-offset", lc.leaderElectionHeadEntryId.Offset).
		Msg("Started leading the shard")
	return &proto.BecomeLeaderResponse{}, nil
}

func (lc *leaderController) AddFollower(req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Epoch != lc.epoch {
		return nil, common.ErrorInvalidEpoch
	}

	if lc.status != proto.ServingStatus_Leader {
		return nil, errors.Wrap(common.ErrorInvalidStatus, "Node is not leader")
	}

	if _, followerAlreadyPresent := lc.followers[req.FollowerName]; followerAlreadyPresent {
		return nil, errors.Errorf("follower %s is already present", req.FollowerName)
	}

	if len(lc.followers) == int(lc.replicationFactor)-1 {
		return nil, errors.New("all followers are already attached")
	}

	if err := lc.addFollower(req.FollowerName, req.FollowerHeadEntryId); err != nil {
		return nil, err
	}

	return &proto.AddFollowerResponse{}, nil
}

func (lc *leaderController) addFollower(follower string, followerHeadEntryId *proto.EntryId) error {
	followerHeadEntryId, err := lc.truncateFollowerIfNeeded(follower, followerHeadEntryId)
	if err != nil {
		lc.log.Error().Err(err).
			Str("follower", follower).
			Interface("follower-head-entry", followerHeadEntryId).
			Int64("epoch", lc.epoch).
			Msg("Failed to truncate follower")
		return err
	}

	cursor, err := NewFollowerCursor(follower, lc.epoch, lc.shardId, lc.rpcClient, lc.quorumAckTracker, lc.wal, lc.db,
		followerHeadEntryId.Offset)
	if err != nil {
		lc.log.Error().Err(err).
			Str("follower", follower).
			Int64("epoch", lc.epoch).
			Msg("Failed to create follower cursor")
		return err
	}

	lc.log.Info().
		Int64("epoch", lc.epoch).
		Interface("leader-election-head-entry", lc.leaderElectionHeadEntryId).
		Str("follower", follower).
		Interface("follower-head-entry", followerHeadEntryId).
		Int64("head-offset", lc.wal.LastOffset()).
		Msg("Added follower")
	lc.followers[follower] = cursor
	lc.followerAckOffsetGauges[follower] = metrics.NewGauge("oxia_server_follower_ack_offset", "", "count",
		map[string]any{
			"shard":    lc.shardId,
			"follower": follower,
		}, func() int64 {
			return cursor.AckOffset()
		})
	return nil
}

func (lc *leaderController) applyAllEntriesIntoDB() error {
	dbCommitOffset, err := lc.db.ReadCommitOffset()
	if err != nil {
		return err
	}

	err = lc.sessionManager.Initialize()
	if err != nil {
		lc.log.Error().Err(err).
			Msg("Failed to initialize session manager")
		return err
	}

	lc.log.Info().
		Int64("commit-offset", dbCommitOffset).
		Int64("head-offset", lc.quorumAckTracker.HeadOffset()).
		Msg("Applying all pending entries to database")

	r, err := lc.wal.NewReader(dbCommitOffset)
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

		if _, err = lc.db.ProcessWrite(writeRequest, entry.Offset, entry.Timestamp, SessionUpdateOperationCallback); err != nil {
			return err
		}
	}

	return nil
}

func (lc *leaderController) truncateFollowerIfNeeded(follower string, followerHeadEntryId *proto.EntryId) (*proto.EntryId, error) {
	lc.log.Debug().
		Int64("epoch", lc.epoch).
		Str("follower", follower).
		Interface("leader-head-entry", lc.leaderElectionHeadEntryId).
		Interface("follower-head-entry", followerHeadEntryId).
		Msg("Needs truncation?")
	if followerHeadEntryId.Epoch == lc.leaderElectionHeadEntryId.Epoch &&
		followerHeadEntryId.Offset <= lc.leaderElectionHeadEntryId.Offset {
		// No need for truncation
		return followerHeadEntryId, nil
	}

	// Coordinator should never send us a follower with an invalid epoch.
	// Checking for sanity here.
	if followerHeadEntryId.Epoch > lc.leaderElectionHeadEntryId.Epoch {
		return nil, common.ErrorInvalidStatus
	}

	lastEntryInFollowerEpoch, err := GetHighestEntryOfEpoch(lc.wal, followerHeadEntryId.Epoch)
	if err != nil {
		return nil, err
	}

	if followerHeadEntryId.Epoch == lastEntryInFollowerEpoch.Epoch &&
		followerHeadEntryId.Offset <= lastEntryInFollowerEpoch.Offset {
		// If the follower is on a previous epoch, but we have the same entry,
		// we don't need to truncate
		lc.log.Debug().
			Int64("epoch", lc.epoch).
			Str("follower", follower).
			Interface("last-entry-in-follower-epoch", lastEntryInFollowerEpoch).
			Interface("follower-head-entry", followerHeadEntryId).
			Msg("No need to truncate follower")
		return followerHeadEntryId, nil
	}

	tr, err := lc.rpcClient.Truncate(follower, &proto.TruncateRequest{
		Epoch:       lc.epoch,
		HeadEntryId: lastEntryInFollowerEpoch,
	})

	if err != nil {
		return nil, err
	} else {
		lc.log.Info().
			Int64("epoch", lc.epoch).
			Str("follower", follower).
			Interface("follower-head-entry", tr.HeadEntryId).
			Msg("Truncated follower")

		return tr.HeadEntryId, nil
	}
}

func (lc *leaderController) Read(request *proto.ReadRequest) (*proto.ReadResponse, error) {
	lc.log.Debug().
		Interface("req", request).
		Msg("Received read request")

	lc.Lock()
	err := checkStatus(proto.ServingStatus_Leader, lc.status)
	lc.Unlock()
	if err != nil {
		return nil, err
	}

	return lc.db.ProcessRead(request)
}

// Write
// A client sends a batch of entries to the leader
//
// A client writes a value from Values to a leader node
// if that value has not previously been written. The leader adds
// the entry to its log, updates its head offset.
func (lc *leaderController) Write(request *proto.WriteRequest) (*proto.WriteResponse, error) {
	_, resp, err := lc.write(func(_ int64) *proto.WriteRequest {
		return request
	})
	return resp, err
}

func (lc *leaderController) write(request func(int64) *proto.WriteRequest) (int64, *proto.WriteResponse, error) {
	timer := lc.writeLatencyHisto.Timer()
	defer timer.Done()

	lc.log.Debug().
		Msg("Write operation")

	timestamp := uint64(time.Now().UnixMilli())

	actualRequest, newOffset, err := lc.appendToWal(request, timestamp)
	if err != nil {
		return wal.InvalidOffset, nil, err
	}

	resp, err := lc.quorumAckTracker.WaitForCommitOffset(newOffset, func() (*proto.WriteResponse, error) {
		return lc.db.ProcessWrite(actualRequest, newOffset, timestamp, SessionUpdateOperationCallback)
	})
	return newOffset, resp, err
}

func (lc *leaderController) appendToWal(request func(int64) *proto.WriteRequest, timestamp uint64) (actualRequest *proto.WriteRequest, offset int64, err error) {
	lc.Lock()

	if err := checkStatus(proto.ServingStatus_Leader, lc.status); err != nil {
		lc.Unlock()
		return nil, wal.InvalidOffset, err
	}

	newOffset := lc.quorumAckTracker.NextOffset()
	actualRequest = request(newOffset)

	lc.log.Debug().
		Interface("req", actualRequest).
		Msg("Append operation")

	value, err := pb.Marshal(actualRequest)
	if err != nil {
		lc.Unlock()
		return actualRequest, wal.InvalidOffset, err
	}
	logEntry := &proto.LogEntry{
		Epoch:     lc.epoch,
		Offset:    newOffset,
		Value:     value,
		Timestamp: timestamp,
	}

	if err = lc.wal.AppendAsync(logEntry); err != nil {
		lc.Unlock()
		return actualRequest, wal.InvalidOffset, errors.Wrap(err, "oxia: failed to append to wal")
	}

	lc.Unlock()

	// Sync the WAL outside the mutex, so that we can have multiple waiting
	// sync requests
	if err = lc.wal.Sync(context.TODO()); err != nil {
		return actualRequest, wal.InvalidOffset, errors.Wrap(err, "oxia: failed to sync the wal")
	}

	lc.quorumAckTracker.AdvanceHeadOffset(newOffset)

	return actualRequest, newOffset, nil
}

func (lc *leaderController) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	// Create a context for handling this stream
	ctx, cancel := context.WithCancel(stream.Context())

	go common.DoWithLabels(map[string]string{
		"oxia":  "dispatch-notifications",
		"shard": fmt.Sprintf("%d", lc.shardId),
		"peer":  common.GetPeer(stream.Context()),
	}, func() {
		if err := lc.dispatchNotifications(ctx, req, stream); err != nil && !errors.Is(err, context.Canceled) {
			lc.log.Warn().Err(err).
				Str("peer", common.GetPeer(stream.Context())).
				Msg("Failed to dispatch notifications")
			cancel()
		}
	})

	select {
	case <-lc.ctx.Done():
		// Leader is getting closed
		cancel()
		return lc.ctx.Err()

	case <-stream.Context().Done():
		// The stream is getting closed
		cancel()
		return stream.Context().Err()
	}
}

func (lc *leaderController) dispatchNotifications(ctx context.Context, req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	lc.log.Debug().
		Interface("start-offset-exclusive", req.StartOffsetExclusive).
		Msg("Dispatch notifications")

	var offsetInclusive int64
	if req.StartOffsetExclusive != nil {
		offsetInclusive = *req.StartOffsetExclusive + 1
	} else {
		commitOffset := lc.quorumAckTracker.CommitOffset()

		// The client is creating a new notification stream and wants to receive the notification from the next
		// entry that will be written.
		// In order to ensure the client will positioned on a given offset, we need to send a first "dummy"
		// notification. The client will wait for this first notification before making the notification
		// channel available to the application
		lc.log.Debug().Int64("commit-offset", commitOffset).Msg("Sending first dummy notification")
		if err := stream.Send(&proto.NotificationBatch{
			ShardId:       lc.shardId,
			Offset:        commitOffset,
			Timestamp:     0,
			Notifications: nil,
		}); err != nil {
			return err
		}

		offsetInclusive = commitOffset + 1
	}

	for ctx.Err() == nil {
		notifications, err := lc.db.ReadNextNotifications(ctx, offsetInclusive)
		if err != nil {
			return err
		}

		lc.log.Debug().
			Int("list-size", len(notifications)).
			Msg("Got a new list of notification batches")

		for _, n := range notifications {
			if err := stream.Send(n); err != nil {
				return err
			}
		}

		offsetInclusive += int64(len(notifications))
	}

	return ctx.Err()
}

func (lc *leaderController) Close() error {
	lc.Lock()
	defer lc.Unlock()

	lc.log.Info().Msg("Closing leader controller")

	lc.status = proto.ServingStatus_NotMember
	lc.cancel()

	var err error
	if lc.quorumAckTracker != nil {
		err = multierr.Append(err, lc.quorumAckTracker.Close())
	}

	for _, follower := range lc.followers {
		err = multierr.Append(err, follower.Close())
	}

	for _, g := range lc.followerAckOffsetGauges {
		g.Unregister()
	}

	err = multierr.Combine(err,
		lc.sessionManager.Close(),
		lc.walTrimmer.Close(),
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

func (lc *leaderController) GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	return &proto.GetStatusResponse{
		Epoch:  lc.epoch,
		Status: lc.status,
	}, nil
}

func (lc *leaderController) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return lc.sessionManager.CreateSession(request)
}

func (lc *leaderController) KeepAlive(sessionId int64, stream proto.OxiaClient_KeepAliveServer) error {
	return lc.sessionManager.KeepAlive(sessionId, stream)
}

func (lc *leaderController) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return lc.sessionManager.CloseSession(request)
}
