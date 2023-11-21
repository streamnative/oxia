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
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"
	"io"
	"sync"
)

type GetResult struct {
	Response *proto.GetResponse
	Err      error
}

type LeaderController interface {
	io.Closer

	Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error)
	Read(ctx context.Context, request *proto.ReadRequest) <-chan GetResult
	List(ctx context.Context, request *proto.ListRequest) (<-chan string, error)
	ListSliceNoMutex(ctx context.Context, request *proto.ListRequest) ([]string, error)

	// NewTerm Handle new term requests
	NewTerm(req *proto.NewTermRequest) (*proto.NewTermResponse, error)

	// BecomeLeader Handles BecomeLeaderRequest from coordinator and prepares to be leader for the shard
	BecomeLeader(ctx context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)

	AddFollower(request *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)

	GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error

	GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	DeleteShard(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)

	// Term The current term of the leader
	Term() int64

	// Status The Status of the leader
	Status() proto.ServingStatus

	CreateSession(*proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(sessionId int64) error
	CloseSession(*proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
}

type leaderController struct {
	sync.RWMutex

	namespace         string
	shardId           int64
	status            proto.ServingStatus
	term              int64
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
	db             kv.DB
	rpcClient      ReplicationRpcProvider
	sessionManager SessionManager
	log            zerolog.Logger

	writeLatencyHisto       metrics.LatencyHistogram
	headOffsetGauge         metrics.Gauge
	commitOffsetGauge       metrics.Gauge
	followerAckOffsetGauges map[string]metrics.Gauge
}

func NewLeaderController(config Config, namespace string, shardId int64, rpcClient ReplicationRpcProvider, walFactory wal.WalFactory, kvFactory kv.KVFactory) (LeaderController, error) {
	labels := metrics.LabelsForShard(namespace, shardId)
	lc := &leaderController{
		status:           proto.ServingStatus_NOT_MEMBER,
		namespace:        namespace,
		shardId:          shardId,
		quorumAckTracker: nil,
		rpcClient:        rpcClient,
		followers:        make(map[string]FollowerCursor),

		writeLatencyHisto: metrics.NewLatencyHistogram("oxia_server_leader_write_latency",
			"Latency for write operations in the leader", labels),
		followerAckOffsetGauges: map[string]metrics.Gauge{},
	}

	lc.headOffsetGauge = metrics.NewGauge("oxia_server_leader_head_offset",
		"The current head offset", "offset", labels, func() int64 {
			qat := lc.quorumAckTracker
			if qat != nil {
				return qat.HeadOffset()
			}

			return -1
		})
	lc.commitOffsetGauge = metrics.NewGauge("oxia_server_leader_commit_offset",
		"The current commit offset", "offset", labels, func() int64 {
			qat := lc.quorumAckTracker
			if qat != nil {
				return qat.CommitOffset()
			}

			return -1
		})

	lc.ctx, lc.cancel = context.WithCancel(context.Background())

	lc.sessionManager = NewSessionManager(lc.ctx, namespace, shardId, lc)

	var err error
	if lc.wal, err = walFactory.NewWal(namespace, shardId, lc); err != nil {
		return nil, err
	}

	if lc.db, err = kv.NewDB(namespace, shardId, kvFactory, config.NotificationsRetentionTime, common.SystemClock); err != nil {
		return nil, err
	}

	if lc.term, err = lc.db.ReadTerm(); err != nil {
		return nil, err
	}

	if lc.term != wal.InvalidTerm {
		lc.status = proto.ServingStatus_FENCED
	}

	lc.setLogger()
	lc.log.Info().Msg("Created leader controller")
	return lc, nil
}

func (lc *leaderController) setLogger() {
	lc.log = log.With().
		Str("component", "leader-controller").
		Str("namespace", lc.namespace).
		Int64("shard", lc.shardId).
		Int64("term", lc.term).
		Logger()
}

func (lc *leaderController) Status() proto.ServingStatus {
	lc.RLock()
	defer lc.RUnlock()
	return lc.status
}

func (lc *leaderController) Term() int64 {
	lc.RLock()
	defer lc.RUnlock()
	return lc.term
}

// NewTerm
//
// # Node handles a new term request
//
// A node receives a new term request, fences itself and responds
// with its head offset.
//
// When a node is fenced it cannot:
//   - accept any writes from a client.
//   - accept add entry append from a leader.
//   - send any entries to followers if it was a leader.
//
// Any existing follow cursors are destroyed as is any state
// regarding reconfigurations.
func (lc *leaderController) NewTerm(req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if lc.isClosed() {
		return nil, common.ErrorAlreadyClosed
	}

	if req.Term < lc.term {
		return nil, common.ErrorInvalidTerm
	} else if req.Term == lc.term && lc.status != proto.ServingStatus_FENCED {
		// It's OK to receive a duplicate Fence request, for the same term, as long as we haven't moved
		// out of the Fenced state for that term
		lc.log.Warn().
			Int64("follower-term", lc.term).
			Int64("new-term", req.Term).
			Interface("status", lc.status).
			Msg("Failed to apply duplicate NewTerm in invalid state")
		return nil, common.ErrorInvalidStatus
	}

	if err := lc.db.UpdateTerm(req.Term); err != nil {
		return nil, err
	}

	lc.term = req.Term
	lc.setLogger()
	lc.status = proto.ServingStatus_FENCED
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
		Msg("Leader successfully initialized in new term")

	return &proto.NewTermResponse{
		HeadEntryId: headEntryId,
	}, nil
}

// BecomeLeader : Node handles a Become Leader request
//
// The node inspects the head offset of each follower and
// compares it to its own head offset, and then either:
//   - Attaches a follow cursor for the follower the head entry ids
//     have the same term, but the follower offset is lower or equal.
//   - Sends a truncate request to the follower if its head
//     entry term does not match the leader's head entry term or has
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
func (lc *leaderController) BecomeLeader(ctx context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if lc.isClosed() {
		return nil, common.ErrorAlreadyClosed
	}

	if lc.status != proto.ServingStatus_FENCED {
		return nil, common.ErrorInvalidStatus
	}

	if req.Term != lc.term {
		return nil, common.ErrorInvalidTerm
	}

	lc.status = proto.ServingStatus_LEADER
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
	lc.sessionManager = NewSessionManager(lc.ctx, lc.namespace, lc.shardId, lc)

	for follower, followerHeadEntryId := range req.FollowerMaps {
		if err := lc.addFollower(follower, followerHeadEntryId); err != nil {
			return nil, err
		}
	}

	// We must wait until all the entries in the leader WAL are fully
	// committed in the quorum, to avoid missing any entries in the DB
	// by the moment we make the leader controller accepting new write/read
	// requests
	if _, err = lc.quorumAckTracker.WaitForCommitOffset(ctx, lc.leaderElectionHeadEntryId.Offset, nil); err != nil {
		return nil, err
	}

	if err = lc.applyAllEntriesIntoDB(); err != nil {
		return nil, err
	}

	lc.log.Info().
		Int64("term", lc.term).
		Int64("head-offset", lc.leaderElectionHeadEntryId.Offset).
		Msg("Started leading the shard")
	return &proto.BecomeLeaderResponse{}, nil
}

func (lc *leaderController) AddFollower(req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Term != lc.term {
		return nil, common.ErrorInvalidTerm
	}

	if lc.status != proto.ServingStatus_LEADER {
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
			Int64("term", lc.term).
			Msg("Failed to truncate follower")
		return err
	}

	cursor, err := NewFollowerCursor(follower, lc.term, lc.namespace, lc.shardId, lc.rpcClient, lc.quorumAckTracker, lc.wal, lc.db,
		followerHeadEntryId.Offset)
	if err != nil {
		lc.log.Error().Err(err).
			Str("follower", follower).
			Int64("term", lc.term).
			Msg("Failed to create follower cursor")
		return err
	}

	lc.log.Info().
		Int64("term", lc.term).
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
		lc.log.Error().Err(err).
			Int64("commit-offset", dbCommitOffset).
			Int64("first-offset", lc.wal.FirstOffset()).
			Msg("Unable to create WAL reader")
		return err
	}
	for r.HasNext() {
		entry, err := r.ReadNext()
		if err != nil {
			return err
		}

		logEntryValue := &proto.LogEntryValue{}
		if err = pb.Unmarshal(entry.Value, logEntryValue); err != nil {
			return err
		}
		for _, writeRequest := range logEntryValue.GetRequests().Writes {
			if _, err = lc.db.ProcessWrite(writeRequest, entry.Offset, entry.Timestamp, SessionUpdateOperationCallback); err != nil {
				return err
			}
		}
	}

	return nil
}

func (lc *leaderController) truncateFollowerIfNeeded(follower string, followerHeadEntryId *proto.EntryId) (*proto.EntryId, error) {
	lc.log.Debug().
		Int64("term", lc.term).
		Str("follower", follower).
		Interface("leader-head-entry", lc.leaderElectionHeadEntryId).
		Interface("follower-head-entry", followerHeadEntryId).
		Msg("Needs truncation?")
	if followerHeadEntryId.Term == lc.leaderElectionHeadEntryId.Term &&
		followerHeadEntryId.Offset <= lc.leaderElectionHeadEntryId.Offset {
		// No need for truncation
		return followerHeadEntryId, nil
	}

	// Coordinator should never send us a follower with an invalid term.
	// Checking for sanity here.
	if followerHeadEntryId.Term > lc.leaderElectionHeadEntryId.Term {
		return nil, common.ErrorInvalidStatus
	}

	lastEntryInFollowerTerm, err := getHighestEntryOfTerm(lc.wal, followerHeadEntryId.Term)
	if err != nil {
		return nil, err
	}

	if followerHeadEntryId.Term == lastEntryInFollowerTerm.Term &&
		followerHeadEntryId.Offset <= lastEntryInFollowerTerm.Offset {
		// If the follower is on a previous term, but we have the same entry,
		// we don't need to truncate
		lc.log.Debug().
			Int64("term", lc.term).
			Str("follower", follower).
			Interface("last-entry-in-follower-term", lastEntryInFollowerTerm).
			Interface("follower-head-entry", followerHeadEntryId).
			Msg("No need to truncate follower")
		return followerHeadEntryId, nil
	}

	tr, err := lc.rpcClient.Truncate(follower, &proto.TruncateRequest{
		Namespace:   lc.namespace,
		ShardId:     lc.shardId,
		Term:        lc.term,
		HeadEntryId: lastEntryInFollowerTerm,
	})

	if err != nil {
		return nil, err
	} else {
		lc.log.Info().
			Int64("term", lc.term).
			Str("follower", follower).
			Interface("follower-head-entry", tr.HeadEntryId).
			Msg("Truncated follower")

		return tr.HeadEntryId, nil
	}
}

func getHighestEntryOfTerm(w wal.Wal, term int64) (*proto.EntryId, error) {
	r, err := w.NewReverseReader()
	if err != nil {
		return InvalidEntryId, err
	}
	defer r.Close()
	for r.HasNext() {
		e, err := r.ReadNext()
		if err != nil {
			return InvalidEntryId, err
		}
		if e.Term <= term {
			return &proto.EntryId{
				Term:   e.Term,
				Offset: e.Offset,
			}, nil
		}
	}
	return InvalidEntryId, nil
}

func (lc *leaderController) Read(ctx context.Context, request *proto.ReadRequest) <-chan GetResult {
	ch := make(chan GetResult)

	lc.RLock()
	err := checkStatus(proto.ServingStatus_LEADER, lc.status)
	lc.RUnlock()
	if err != nil {
		go func() {
			ch <- GetResult{Err: err}
		}()
		return ch
	}

	go lc.read(ctx, request, ch)

	return ch
}

func (lc *leaderController) read(ctx context.Context, request *proto.ReadRequest, ch chan<- GetResult) {
	common.DoWithLabels(map[string]string{
		"oxia":  "read",
		"shard": fmt.Sprintf("%d", lc.shardId),
		"peer":  common.GetPeer(ctx),
	}, func() {
		lc.log.Debug().
			Msg("Received read request")

		for _, get := range request.Gets {
			response, err := lc.db.Get(get)
			if err != nil {
				return
			}
			ch <- GetResult{Response: response}
			if ctx.Err() != nil {
				ch <- GetResult{Err: ctx.Err()}
				break
			}
		}
		close(ch)
	})
}

func (lc *leaderController) List(ctx context.Context, request *proto.ListRequest) (<-chan string, error) {
	ch := make(chan string)

	lc.RLock()
	err := checkStatus(proto.ServingStatus_LEADER, lc.status)
	lc.RUnlock()
	if err != nil {
		return nil, err
	}

	go lc.list(ctx, request, ch)

	return ch, nil
}

func (lc *leaderController) list(ctx context.Context, request *proto.ListRequest, ch chan<- string) {
	common.DoWithLabels(map[string]string{
		"oxia":  "list",
		"shard": fmt.Sprintf("%d", lc.shardId),
		"peer":  common.GetPeer(ctx),
	}, func() {
		lc.log.Debug().
			Msg("Received list request")

		it, err := lc.db.List(request)
		if err != nil {
			lc.log.Warn().Err(err).
				Msg("Failed to process list request")
			close(ch)
			return
		}

		defer func() {
			_ = it.Close()
		}()

		for ; it.Valid(); it.Next() {
			ch <- it.Key()
			if ctx.Err() != nil {
				break
			}
		}
		close(ch)
	})
}

func (lc *leaderController) ListSliceNoMutex(ctx context.Context, request *proto.ListRequest) ([]string, error) {
	ch := make(chan string)
	go lc.list(ctx, request, ch)
	keys := make([]string, 0)
	for {
		select {
		case key, more := <-ch:
			if !more {
				return keys, nil
			}
			keys = append(keys, key)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Write
// A client sends a batch of entries to the leader
//
// A client writes a value from Values to a leader node
// if that value has not previously been written. The leader adds
// the entry to its log, updates its head offset.
func (lc *leaderController) Write(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	_, resp, err := lc.write(ctx, func(_ int64) *proto.WriteRequest {
		return request
	})
	return resp, err
}

func (lc *leaderController) write(ctx context.Context, request func(int64) *proto.WriteRequest) (int64, *proto.WriteResponse, error) {
	timer := lc.writeLatencyHisto.Timer()
	defer timer.Done()

	lc.log.Debug().
		Msg("Write operation")

	actualRequest, newOffset, timestamp, err := lc.appendToWal(ctx, request)
	if err != nil {
		return wal.InvalidOffset, nil, err
	}

	resp, err := lc.quorumAckTracker.WaitForCommitOffset(ctx, newOffset, func() (*proto.WriteResponse, error) {
		return lc.db.ProcessWrite(actualRequest, newOffset, timestamp, SessionUpdateOperationCallback)
	})
	return newOffset, resp, err
}

func (lc *leaderController) appendToWal(ctx context.Context, request func(int64) *proto.WriteRequest) (actualRequest *proto.WriteRequest, offset int64, timestamp uint64, err error) {
	lc.Lock()

	if err := checkStatus(proto.ServingStatus_LEADER, lc.status); err != nil {
		lc.Unlock()
		return nil, wal.InvalidOffset, 0, err
	}

	newOffset := lc.quorumAckTracker.NextOffset()
	actualRequest = request(newOffset)

	lc.log.Debug().
		Interface("req", actualRequest).
		Msg("Append operation")

	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	logEntryValue.Value = &proto.LogEntryValue_Requests{
		Requests: &proto.WriteRequests{
			Writes: []*proto.WriteRequest{actualRequest},
		},
	}
	value, err := logEntryValue.MarshalVT()
	if err != nil {
		lc.Unlock()
		return actualRequest, wal.InvalidOffset, timestamp, err
	}
	logEntry := &proto.LogEntry{
		Term:      lc.term,
		Offset:    newOffset,
		Value:     value,
		Timestamp: timestamp,
	}

	if err = lc.wal.AppendAsync(logEntry); err != nil {
		lc.Unlock()
		return actualRequest, wal.InvalidOffset, timestamp, errors.Wrap(err, "oxia: failed to append to wal")
	}

	lc.Unlock()

	// Sync the WAL outside the mutex, so that we can have multiple waiting
	// sync requests
	if err = lc.wal.Sync(ctx); err != nil {
		return actualRequest, wal.InvalidOffset, timestamp, errors.Wrap(err, "oxia: failed to sync the wal")
	}
	lc.quorumAckTracker.AdvanceHeadOffset(newOffset)
	return actualRequest, newOffset, timestamp, nil

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

func (lc *leaderController) isClosed() bool {
	return lc.ctx.Err() != nil
}

func (lc *leaderController) Close() error {
	lc.Lock()
	defer lc.Unlock()
	return lc.close()
}

func (lc *leaderController) close() error {
	lc.log.Info().Msg("Closing leader controller")

	lc.status = proto.ServingStatus_NOT_MEMBER
	lc.cancel()

	var err error
	for _, follower := range lc.followers {
		err = multierr.Append(err, follower.Close())
	}
	lc.followers = nil

	for _, g := range lc.followerAckOffsetGauges {
		g.Unregister()
	}
	lc.followerAckOffsetGauges = map[string]metrics.Gauge{}

	err = lc.sessionManager.Close()

	if lc.wal != nil {
		err = multierr.Append(err, lc.wal.Close())
		lc.wal = nil
	}

	if lc.db != nil {
		err = multierr.Append(err, lc.db.Close())
		lc.db = nil
	}

	if lc.quorumAckTracker != nil {
		err = multierr.Append(err, lc.quorumAckTracker.Close())
		lc.quorumAckTracker = nil
	}

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
	return &proto.EntryId{Term: entry.Term, Offset: entry.Offset}, nil
}

func (lc *leaderController) CommitOffset() int64 {
	qat := lc.quorumAckTracker
	if qat != nil {
		return qat.CommitOffset()
	}
	return wal.InvalidOffset
}

func (lc *leaderController) GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	lc.RLock()
	defer lc.RUnlock()

	var (
		headOffset   = wal.InvalidOffset
		commitOffset = wal.InvalidOffset
	)
	if lc.quorumAckTracker != nil {
		headOffset = lc.quorumAckTracker.HeadOffset()
		commitOffset = lc.quorumAckTracker.CommitOffset()
	}

	return &proto.GetStatusResponse{
		Term:         lc.term,
		Status:       lc.status,
		HeadOffset:   headOffset,
		CommitOffset: commitOffset,
	}, nil
}

func (lc *leaderController) DeleteShard(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if request.Term != lc.term {
		return nil, common.ErrorInvalidTerm
	}

	lc.log.Info().Msg("Deleting shard")

	// Wipe out both WAL and DB contents
	if err := multierr.Combine(
		lc.wal.Delete(),
		lc.db.Delete(),
	); err != nil {
		return nil, err
	}

	lc.db = nil
	lc.wal = nil
	if err := lc.close(); err != nil {
		return nil, err
	}

	return &proto.DeleteShardResponse{}, nil
}

func (lc *leaderController) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return lc.sessionManager.CreateSession(request)
}

func (lc *leaderController) KeepAlive(sessionId int64) error {
	return lc.sessionManager.KeepAlive(sessionId)
}

func (lc *leaderController) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return lc.sessionManager.CloseSession(request)
}
