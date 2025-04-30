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
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streamnative/oxia/common/entities"
	"go.uber.org/multierr"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common/channel"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/callback"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
)

var ErrLeaderClosed = errors.New("the leader has been closed")

type GetResult struct {
	Response *proto.GetResponse
	Err      error
}

type LeaderController interface {
	io.Closer

	Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error)
	WriteStream(stream proto.OxiaClient_WriteStreamServer) error
	Read(ctx context.Context, request *proto.ReadRequest) <-chan GetResult
	List(ctx context.Context, request *proto.ListRequest, cb callback.StreamCallback[string])
	ListBlock(ctx context.Context, request *proto.ListRequest) ([]string, error)
	RangeScan(ctx context.Context, request *proto.RangeScanRequest, cb callback.StreamCallback[*proto.GetResponse])

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
	termOptions    kv.TermOptions
	rpcClient      ReplicationRpcProvider
	sessionManager SessionManager
	log            *slog.Logger

	writeLatencyHisto       metrics.LatencyHistogram
	headOffsetGauge         metrics.Gauge
	commitOffsetGauge       metrics.Gauge
	followerAckOffsetGauges map[string]metrics.Gauge

	notificationDispatchers map[int64]*notificationDispatcher
}

func NewLeaderController(config Config, namespace string, shardId int64, rpcClient ReplicationRpcProvider, walFactory wal.Factory, kvFactory kv.Factory) (LeaderController, error) {
	labels := metrics.LabelsForShard(namespace, shardId)
	lc := &leaderController{
		status:                  proto.ServingStatus_NOT_MEMBER,
		namespace:               namespace,
		shardId:                 shardId,
		quorumAckTracker:        nil,
		rpcClient:               rpcClient,
		followers:               make(map[string]FollowerCursor),
		notificationDispatchers: make(map[int64]*notificationDispatcher),

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

	if lc.term, lc.termOptions, err = lc.db.ReadTerm(); err != nil {
		return nil, err
	}

	if lc.term != wal.InvalidTerm {
		lc.status = proto.ServingStatus_FENCED
	}

	lc.db.EnableNotifications(lc.termOptions.NotificationsEnabled)
	lc.setLogger()
	lc.log.Info("Created leader controller")
	return lc, nil
}

func (lc *leaderController) setLogger() {
	lc.log = slog.With(
		slog.String("component", "leader-controller"),
		slog.String("namespace", lc.namespace),
		slog.Int64("shard", lc.shardId),
		slog.Int64("term", lc.term),
	)
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
		return nil, common.ErrAlreadyClosed
	}

	if req.Term < lc.term {
		return nil, common.ErrInvalidTerm
	} else if req.Term == lc.term && lc.status != proto.ServingStatus_FENCED {
		// It's OK to receive a duplicate Fence request, for the same term, as long as we haven't moved
		// out of the Fenced state for that term
		lc.log.Warn(
			"Failed to apply duplicate NewTerm in invalid state",
			slog.Int64("follower-term", lc.term),
			slog.Int64("new-term", req.Term),
			slog.Any("status", lc.status),
		)
		return nil, common.ErrInvalidStatus
	}

	lc.termOptions = kv.ToDbOption(req.Options)
	if err := lc.db.UpdateTerm(req.Term, lc.termOptions); err != nil {
		return nil, err
	}

	lc.db.EnableNotifications(lc.termOptions.NotificationsEnabled)
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

	lc.log.Info(
		"Leader successfully initialized in new term",
		slog.Any("last-entry", headEntryId),
	)

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
		return nil, common.ErrAlreadyClosed
	}

	if lc.status != proto.ServingStatus_FENCED {
		return nil, common.ErrInvalidStatus
	}

	if req.Term != lc.term {
		return nil, common.ErrInvalidTerm
	}

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
		if err := lc.addFollower(follower, followerHeadEntryId); err != nil { //nolint:contextcheck
			return nil, err
		}
	}

	// We must wait until all the entries in the leader WAL are fully
	// committed in the quorum, to avoid missing any entries in the DB
	// by the moment we make the leader controller accepting new write/read
	// requests
	if err = lc.quorumAckTracker.WaitForCommitOffset(ctx, lc.leaderElectionHeadEntryId.Offset); err != nil {
		return nil, err
	}

	if err = lc.applyAllEntriesIntoDB(); err != nil {
		return nil, err
	}

	lc.log.Info(
		"Started leading the shard",
		slog.Int64("term", lc.term),
		slog.Int64("head-offset", lc.leaderElectionHeadEntryId.Offset),
	)

	lc.status = proto.ServingStatus_LEADER
	return &proto.BecomeLeaderResponse{}, nil
}

func (lc *leaderController) AddFollower(req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	if req.Term != lc.term {
		return nil, common.ErrInvalidTerm
	}

	if lc.status != proto.ServingStatus_LEADER {
		return nil, errors.Wrap(common.ErrInvalidStatus, "Node is not leader")
	}

	if _, followerAlreadyPresent := lc.followers[req.FollowerName]; followerAlreadyPresent {
		return &proto.AddFollowerResponse{}, nil
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
		lc.log.Error(
			"Failed to truncate follower",
			slog.Any("error", err),
			slog.String("follower", follower),
			slog.Any("follower-head-entry", followerHeadEntryId),
			slog.Int64("term", lc.term),
		)
		return err
	}

	cursor, err := NewFollowerCursor(follower, lc.term, lc.namespace, lc.shardId, lc.rpcClient, lc.quorumAckTracker, lc.wal, lc.db,
		followerHeadEntryId.Offset)
	if err != nil {
		lc.log.Error(
			"Failed to create follower cursor",
			slog.Any("error", err),
			slog.String("follower", follower),
			slog.Int64("term", lc.term),
		)
		return err
	}

	lc.log.Info(
		"Added follower",
		slog.Int64("term", lc.term),
		slog.Any("leader-election-head-entry", lc.leaderElectionHeadEntryId),
		slog.String("follower", follower),
		slog.Any("follower-head-entry", followerHeadEntryId),
		slog.Int64("head-offset", lc.wal.LastOffset()),
	)
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

func (lc *leaderController) applyAllEntriesIntoDBLoop(r wal.Reader) error {
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
			if _, err = lc.db.ProcessWrite(writeRequest, entry.Offset, entry.Timestamp, WrapperUpdateOperationCallback); err != nil {
				return err
			}
		}
	}

	return nil
}

func (lc *leaderController) applyAllEntriesIntoDB() error {
	dbCommitOffset, err := lc.db.ReadCommitOffset()
	if err != nil {
		return err
	}

	lc.log.Info(
		"Applying all pending entries to database",
		slog.Int64("commit-offset", dbCommitOffset),
		slog.Int64("head-offset", lc.quorumAckTracker.HeadOffset()),
	)

	r, err := lc.wal.NewReader(dbCommitOffset)
	if err != nil {
		lc.log.Error(
			"Unable to create WAL reader",
			slog.Any("error", err),
			slog.Int64("commit-offset", dbCommitOffset),
			slog.Int64("first-offset", lc.wal.FirstOffset()),
		)
		return err
	}

	if err = lc.applyAllEntriesIntoDBLoop(r); err != nil {
		return errors.Wrap(err, "failed to applies wal entries to db")
	}

	if err = lc.sessionManager.Initialize(); err != nil {
		lc.log.Error(
			"Failed to initialize session manager",
			slog.Any("error", err),
		)
		return err
	}
	return nil
}

func (lc *leaderController) truncateFollowerIfNeeded(follower string, followerHeadEntryId *proto.EntryId) (*proto.EntryId, error) {
	lc.log.Debug(
		"Needs truncation?",
		slog.Int64("term", lc.term),
		slog.String("follower", follower),
		slog.Any("leader-head-entry", lc.leaderElectionHeadEntryId),
		slog.Any("follower-head-entry", followerHeadEntryId),
	)
	if followerHeadEntryId.Term == lc.leaderElectionHeadEntryId.Term &&
		followerHeadEntryId.Offset <= lc.leaderElectionHeadEntryId.Offset {
		// No need for truncation
		return followerHeadEntryId, nil
	}

	// Coordinator should never send us a follower with an invalid term.
	// Checking for sanity here.
	if followerHeadEntryId.Term > lc.leaderElectionHeadEntryId.Term {
		return nil, common.ErrInvalidStatus
	}

	lastEntryInFollowerTerm, err := getHighestEntryOfTerm(lc.wal, followerHeadEntryId.Term)
	if err != nil {
		return nil, err
	}

	if followerHeadEntryId.Term == lastEntryInFollowerTerm.Term &&
		followerHeadEntryId.Offset <= lastEntryInFollowerTerm.Offset {
		// If the follower is on a previous term, but we have the same entry,
		// we don't need to truncate
		lc.log.Debug(
			"No need to truncate follower",
			slog.Int64("term", lc.term),
			slog.String("follower", follower),
			slog.Any("last-entry-in-follower-term", lastEntryInFollowerTerm),
			slog.Any("follower-head-entry", followerHeadEntryId),
		)
		return followerHeadEntryId, nil
	}

	tr, err := lc.rpcClient.Truncate(follower, &proto.TruncateRequest{
		Namespace:   lc.namespace,
		Shard:       lc.shardId,
		Term:        lc.term,
		HeadEntryId: lastEntryInFollowerTerm,
	})

	if err != nil {
		return nil, err
	}

	lc.log.Info(
		"Truncated follower",
		slog.Int64("term", lc.term),
		slog.String("follower", follower),
		slog.Any("follower-head-entry", tr.HeadEntryId),
	)

	return tr.HeadEntryId, nil
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
	err := checkStatusIsLeader(lc.status)
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
	common.DoWithLabels(
		ctx,
		map[string]string{
			"oxia":  "read",
			"shard": fmt.Sprintf("%d", lc.shardId),
			"peer":  common.GetPeer(ctx),
		},
		func() {
			lc.log.Debug("Received read request")

			defer close(ch)
			for _, get := range request.Gets {
				response, err := lc.db.Get(get)
				if err != nil {
					ch <- GetResult{Err: ctx.Err()}
					return
				}
				ch <- GetResult{Response: response}
				if ctx.Err() != nil {
					ch <- GetResult{Err: ctx.Err()}
					break
				}
			}
		},
	)
}

func (lc *leaderController) List(ctx context.Context, request *proto.ListRequest, cb callback.StreamCallback[string]) {
	lc.RLock()
	err := checkStatusIsLeader(lc.status)
	lc.RUnlock()
	if err != nil {
		cb.OnComplete(err)
		return
	}
	lc.list(ctx, request, cb)
}

func (lc *leaderController) list(ctx context.Context, request *proto.ListRequest, cb callback.StreamCallback[string]) {
	go common.DoWithLabels(
		ctx,
		map[string]string{
			"oxia":  "list",
			"shard": fmt.Sprintf("%d", lc.shardId),
			"peer":  common.GetPeer(ctx),
		},
		func() {
			lc.log.Debug("Received list request", slog.Any("request", request))

			var it kv.KeyIterator
			var err error
			if request.SecondaryIndexName != nil {
				it, err = newSecondaryIndexListIterator(request, lc.db)
			} else {
				it, err = lc.db.List(request)
			}
			if err != nil {
				lc.log.Warn(
					"Failed to process list request",
					slog.Any("error", err),
				)
				cb.OnComplete(err)
				return
			}

			defer func() {
				_ = it.Close()
			}()

			for ; it.Valid(); it.Next() {
				if err = cb.OnNext(it.Key()); err != nil {
					cb.OnComplete(err)
					return
				}
				if ctx.Err() != nil {
					break
				}
			}
			cb.OnComplete(nil)
		},
	)
}

func (lc *leaderController) ListBlock(ctx context.Context, request *proto.ListRequest) ([]string, error) {
	// todo: support leader status check without lock
	ch := make(chan *entities.TWithError[string])
	go lc.list(ctx, request, callback.ReadFromStreamCallback(ch))
	return channel.ReadAll(ctx, ch)
}

func (lc *leaderController) RangeScan(ctx context.Context, request *proto.RangeScanRequest, cb callback.StreamCallback[*proto.GetResponse]) {
	lc.RLock()
	err := checkStatusIsLeader(lc.status)
	lc.RUnlock()
	if err != nil {
		cb.OnComplete(err)
		return
	}

	go common.DoWithLabels(ctx,
		map[string]string{
			"oxia":  "range-scan",
			"shard": fmt.Sprintf("%d", lc.shardId),
			"peer":  common.GetPeer(ctx),
		},
		func() {
			lc.log.Debug("Received list request", slog.Any("request", request))

			var it kv.RangeScanIterator
			var err error
			if request.SecondaryIndexName != nil {
				it, err = newSecondaryIndexRangeScanIterator(request, lc.db)
			} else {
				it, err = lc.db.RangeScan(request)
			}

			if err != nil {
				lc.log.Warn("Failed to process range-scan request", slog.Any("error", err))
				cb.OnComplete(err)
				return
			}

			defer func() {
				_ = it.Close()
			}()

			for ; it.Valid(); it.Next() {
				gr, err := it.Value()
				if err != nil {
					cb.OnComplete(err)
					return
				}
				if err = cb.OnNext(gr); err != nil {
					cb.OnComplete(err)
					return
				}
				if ctx.Err() != nil {
					break
				}
			}
			cb.OnComplete(nil)
		},
	)
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
	defer timer.Done() //nolint:contextcheck

	lc.log.Debug("Write operation")

	actualRequest, newOffset, timestamp, err := lc.appendToWal(ctx, request)
	if err != nil {
		return wal.InvalidOffset, nil, err
	}

	if err := lc.quorumAckTracker.WaitForCommitOffset(ctx, newOffset); err != nil {
		return wal.InvalidOffset, nil, err
	}
	writeResponse, err := lc.db.ProcessWrite(actualRequest, newOffset, timestamp, WrapperUpdateOperationCallback)
	return newOffset, writeResponse, err
}

func (lc *leaderController) appendToWal(ctx context.Context, request func(int64) *proto.WriteRequest) (actualRequest *proto.WriteRequest, offset int64, timestamp uint64, err error) {
	lc.Lock()

	if err := checkStatusIsLeader(lc.status); err != nil {
		lc.Unlock()
		return nil, wal.InvalidOffset, 0, err
	}

	newOffset := lc.quorumAckTracker.NextOffset()
	timestamp = uint64(time.Now().UnixMilli())
	actualRequest = request(newOffset)

	lc.log.Debug(
		"Append operation",
		slog.Any("req", actualRequest),
	)

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

func (lc *leaderController) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	if err := checkStatusIsLeader(lc.status); err != nil {
		return err
	}

	closeStreamCh := make(chan error, 1)

	go common.DoWithLabels(
		stream.Context(),
		map[string]string{
			"oxia":      "write-stream",
			"namespace": lc.namespace,
			"shard":     fmt.Sprintf("%d", lc.shardId),
		},
		func() { lc.handleWriteStream(stream, closeStreamCh) },
	)

	select {
	case err := <-closeStreamCh:
		return err
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-lc.ctx.Done():
		return errors.Wrap(ErrLeaderClosed, lc.ctx.Err().Error())
	}
}

func (lc *leaderController) handleWriteStream(stream proto.OxiaClient_WriteStreamServer,
	closeCh chan error) {
	for {
		req, err := stream.Recv()

		if err != nil {
			sendNonBlocking(closeCh, err)
			return
		} else if req == nil {
			sendNonBlocking(closeCh, errors.New("stream closed"))
			return
		}

		timer := lc.writeLatencyHisto.Timer()
		slog.Debug("Got request in stream",
			slog.Any("req", req))

		lc.appendToWalStreamRequest(req, func(offset int64, timestamp uint64, err error) {
			lc.handleWalSynced(stream, req, closeCh, offset, timestamp, err, timer)
		})
	}
}

func (lc *leaderController) handleWalSynced(stream proto.OxiaClient_WriteStreamServer,
	req *proto.WriteRequest, closeCh chan error,
	offset int64, timestamp uint64, err error, timer metrics.Timer) {
	if err != nil {
		timer.Done()
		sendNonBlocking(closeCh, err)
		return
	}

	lc.RLock()
	tracker := lc.quorumAckTracker
	lc.RUnlock()

	if tracker == nil {
		timer.Done()
		sendNonBlocking(closeCh, common.ErrInvalidStatus)
		return
	}

	tracker.WaitForCommitOffsetAsync(context.Background(), offset, callback.NewOnce(
		func(_ any) {
			defer timer.Done()
			localResponse, err := lc.db.ProcessWrite(req, offset, timestamp, WrapperUpdateOperationCallback)
			if err != nil {
				sendNonBlocking(closeCh, err)
				return
			}
			if err = stream.Send(localResponse); err != nil {
				sendNonBlocking(closeCh, err)
				return
			}
		},
		func(err error) {
			defer timer.Done()
			sendNonBlocking(closeCh, err)
		},
	))
}

func (lc *leaderController) appendToWalStreamRequest(request *proto.WriteRequest,
	cb func(offset int64, timestamp uint64, err error)) {
	lc.Lock()

	if err := checkStatusIsLeader(lc.status); err != nil {
		lc.Unlock()
		cb(wal.InvalidOffset, 0, err)
		return
	}

	newOffset := lc.quorumAckTracker.NextOffset()
	log := lc.wal
	tracker := lc.quorumAckTracker
	lc.Unlock()

	timestamp := uint64(time.Now().UnixMilli())
	lc.log.Debug(
		"Append operation",
		slog.Any("req", request),
	)

	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	logEntryValue.Value = &proto.LogEntryValue_Requests{
		Requests: &proto.WriteRequests{
			Writes: []*proto.WriteRequest{request},
		},
	}
	value, err := logEntryValue.MarshalVT()
	if err != nil {
		cb(wal.InvalidOffset, timestamp, err)
		return
	}
	logEntry := &proto.LogEntry{
		Term:      lc.term,
		Offset:    newOffset,
		Value:     value,
		Timestamp: timestamp,
	}

	log.AppendAndSync(logEntry, func(err error) {
		if err != nil {
			cb(wal.InvalidOffset, timestamp, errors.Wrap(err, "oxia: failed to append to wal"))
		} else {
			tracker.AdvanceHeadOffset(newOffset)
			cb(newOffset, timestamp, nil)
		}
	})
}

// ////

func (lc *leaderController) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	if !lc.termOptions.NotificationsEnabled {
		return common.ErrNotificationsNotEnabled
	}
	return startNotificationDispatcher(lc, req, stream)
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
	lc.log.Info("Closing leader controller")

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

	for _, nd := range lc.notificationDispatchers {
		nd.close()
	}

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

func getLastEntryIdInWal(walObject wal.Wal) (*proto.EntryId, error) {
	reader, err := walObject.NewReverseReader()
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

func (lc *leaderController) GetStatus(_ *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
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

	if request.Term < lc.term {
		lc.log.Warn("Invalid term when deleting shard",
			slog.Int64("follower-term", lc.term),
			slog.Int64("new-term", request.Term))
		_ = lc.close()
		return nil, common.ErrInvalidTerm
	}

	lc.log.Info("Deleting shard")
	deleteWal := lc.wal
	deleteDb := lc.db

	// close the leader controller first
	if err := lc.close(); err != nil {
		return nil, err
	}

	// Wipe out both WAL and DB contents
	if err := multierr.Combine(
		deleteWal.Delete(),
		deleteDb.Delete(),
	); err != nil {
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

func checkStatusIsLeader(actual proto.ServingStatus) error {
	if actual != proto.ServingStatus_LEADER {
		return status.Errorf(common.CodeInvalidStatus, "Received message in the wrong state. In %+v, should be %+v.", actual, proto.ServingStatus_LEADER)
	}
	return nil
}

func sendNonBlocking(ch chan error, err error) {
	select {
	case ch <- err:
	default:
	}
}
