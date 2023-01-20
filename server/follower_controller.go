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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"
	"io"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
	"sync/atomic"
)

// FollowerController handles all the operations of a given shard's follower
type FollowerController interface {
	io.Closer

	// Fence
	//
	// Node handles a fence request
	//
	// A node receives a fencing request, fences itself and responds
	// with its head offset.
	//
	// When a node is fenced it cannot:
	// - accept any writes from a client.
	// - accept append from a leader.
	// - send any entries to followers if it was a leader.
	//
	// Any existing follow cursors are destroyed as is any state
	//regarding reconfigurations.
	Fence(req *proto.FenceRequest) (*proto.FenceResponse, error)

	// Truncate
	//
	// A node that receives a truncate request knows that it
	// has been selected as a follower. It truncates its log
	// to the indicates entry id, updates its term and changes
	// to a Follower.
	Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error)

	Replicate(stream proto.OxiaLogReplication_ReplicateServer) error

	SendSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error

	GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error)

	Term() int64
	CommitOffset() int64
	Status() proto.ServingStatus
}

type followerController struct {
	sync.Mutex

	shardId uint32
	term    int64

	// The highest commit offset advertised by the leader
	advertisedCommitOffset atomic.Int64

	// The commit offset already applied in the database
	commitOffset atomic.Int64

	// Offset of the last entry appended and not fully synced yet on the wal
	lastAppendedOffset int64

	status     proto.ServingStatus
	wal        wal.Wal
	walTrimmer wal.Trimmer
	kvFactory  kv.KVFactory
	db         kv.DB

	ctx              context.Context
	cancel           context.CancelFunc
	syncCond         common.ConditionContext
	applyEntriesCond common.ConditionContext
	closeStreamWg    common.WaitGroup
	log              zerolog.Logger
	config           Config

	writeLatencyHisto metrics.LatencyHistogram
}

func NewFollowerController(config Config, shardId uint32, wf wal.WalFactory, kvFactory kv.KVFactory) (FollowerController, error) {
	fc := &followerController{
		config:        config,
		shardId:       shardId,
		kvFactory:     kvFactory,
		status:        proto.ServingStatus_NotMember,
		closeStreamWg: nil,
		log: log.With().
			Str("component", "follower-controller").
			Uint32("shard", shardId).
			Logger(),
		writeLatencyHisto: metrics.NewLatencyHistogram("oxia_server_follower_write_latency",
			"Latency for write operations in the follower", metrics.LabelsForShard(shardId)),
	}
	fc.ctx, fc.cancel = context.WithCancel(context.Background())
	fc.syncCond = common.NewConditionContext(fc)
	fc.applyEntriesCond = common.NewConditionContext(fc)

	var err error
	if fc.wal, err = wf.NewWal(shardId); err != nil {
		return nil, err
	}

	fc.lastAppendedOffset = fc.wal.LastOffset()
	fc.walTrimmer = wal.NewTrimmer(shardId, fc.wal, config.WalRetentionTime, wal.DefaultCheckInterval, common.SystemClock)

	if fc.db, err = kv.NewDB(shardId, kvFactory, config.NotificationsRetentionTime, common.SystemClock); err != nil {
		return nil, err
	}

	if fc.term, err = fc.db.ReadTerm(); err != nil {
		return nil, err
	}

	if fc.term != wal.InvalidTerm {
		fc.status = proto.ServingStatus_Fenced
	}

	commitOffset, err := fc.db.ReadCommitOffset()
	if err != nil {
		return nil, err
	}
	fc.commitOffset.Store(commitOffset)

	fc.log = fc.log.With().Int64("term", fc.term).Logger()

	go common.DoWithLabels(map[string]string{
		"oxia":  "follower-apply-committed-entries",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, fc.applyAllCommittedEntries)

	fc.log.Info().
		Int64("head-offset", fc.wal.LastOffset()).
		Int64("commit-offset", commitOffset).
		Msg("Created follower")
	return fc, nil
}

func (fc *followerController) Close() error {
	fc.Lock()
	defer fc.Unlock()

	fc.log.Debug().Msg("Closing follower controller")
	fc.cancel()

	err := multierr.Combine(
		fc.walTrimmer.Close(),
		fc.wal.Close(),
	)

	if fc.db != nil {
		err = multierr.Append(err, fc.db.Close())
	}
	return err
}

func (fc *followerController) closeStream(err error) {
	fc.Lock()
	defer fc.Unlock()

	fc.closeStreamNoMutex(err)
}

func (fc *followerController) closeStreamNoMutex(err error) {
	if err != nil && err != io.EOF && status.Code(err) != codes.Canceled {
		fc.log.Warn().Err(err).
			Msg("Error in handle Replicate stream")
	}

	if fc.closeStreamWg != nil {
		fc.closeStreamWg.Fail(err)
		fc.closeStreamWg = nil
	}
}

func (fc *followerController) Status() proto.ServingStatus {
	fc.Lock()
	defer fc.Unlock()
	return fc.status
}

func (fc *followerController) Term() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.term
}

func (fc *followerController) CommitOffset() int64 {
	return fc.commitOffset.Load()
}

func (fc *followerController) Fence(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if req.Term < fc.term {
		fc.log.Warn().
			Int64("follower-term", fc.term).
			Int64("fence-term", req.Term).
			Msg("Failed to fence with invalid term")
		return nil, common.ErrorInvalidTerm
	} else if req.Term == fc.term && fc.status != proto.ServingStatus_Fenced {
		// It's OK to receive a duplicate Fence request, for the same term, as long as we haven't moved
		// out of the Fenced state for that term
		fc.log.Warn().
			Int64("follower-term", fc.term).
			Int64("fence-term", req.Term).
			Interface("status", fc.status).
			Msg("Failed to fence with same term in invalid state")
		return nil, common.ErrorInvalidStatus
	}

	if err := fc.db.UpdateTerm(req.Term); err != nil {
		return nil, err
	}

	fc.term = req.Term
	fc.log = fc.log.With().Int64("term", fc.term).Logger()
	fc.status = proto.ServingStatus_Fenced
	fc.closeStreamNoMutex(nil)

	lastEntryId, err := getLastEntryIdInWal(fc.wal)
	if err != nil {
		fc.log.Warn().Err(err).
			Int64("follower-term", fc.term).
			Int64("fence-term", req.Term).
			Msg("Failed to get last")
		return nil, err
	}

	fc.log.Info().
		Interface("last-entry", lastEntryId).
		Msg("Follower successfully fenced")
	return &proto.FenceResponse{HeadEntryId: lastEntryId}, nil
}

func (fc *followerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if fc.status != proto.ServingStatus_Fenced {
		return nil, common.ErrorInvalidStatus
	}

	if req.Term != fc.term {
		return nil, common.ErrorInvalidTerm
	}

	fc.status = proto.ServingStatus_Follower
	headOffset, err := fc.wal.TruncateLog(req.HeadEntryId.Offset)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to truncate wal. truncate-offset: %d - wal-last-offset: %d",
			req.HeadEntryId.Offset, fc.wal.LastOffset())
	}

	return &proto.TruncateResponse{
		HeadEntryId: &proto.EntryId{
			Term:   req.Term,
			Offset: headOffset,
		},
	}, nil
}

func (fc *followerController) Replicate(stream proto.OxiaLogReplication_ReplicateServer) error {
	fc.Lock()
	if fc.status != proto.ServingStatus_Fenced && fc.status != proto.ServingStatus_Follower {
		fc.Unlock()
		return common.ErrorInvalidStatus
	}

	if fc.closeStreamWg != nil {
		fc.Unlock()
		return common.ErrorLeaderAlreadyConnected
	}

	closeStreamWg := common.NewWaitGroup(1)
	fc.closeStreamWg = closeStreamWg
	fc.Unlock()

	go common.DoWithLabels(map[string]string{
		"oxia":  "add-entries",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() { fc.handleServerStream(stream) })

	go common.DoWithLabels(map[string]string{
		"oxia":  "add-entries-sync",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() { fc.handleReplicateSync(stream) })

	return closeStreamWg.Wait(fc.ctx)
}

func (fc *followerController) handleServerStream(stream proto.OxiaLogReplication_ReplicateServer) {
	for {
		if req, err := stream.Recv(); err != nil {
			fc.closeStream(err)
			return
		} else if req == nil {
			fc.closeStream(nil)
			return
		} else if err := fc.append(req, stream); err != nil {
			fc.closeStream(err)
			return
		}
	}
}

func (fc *followerController) append(req *proto.Append, stream proto.OxiaLogReplication_ReplicateServer) error {
	timer := fc.writeLatencyHisto.Timer()
	defer timer.Done()

	fc.Lock()
	defer fc.Unlock()

	if req.Term != fc.term {
		return common.ErrorInvalidTerm
	}

	fc.log.Debug().
		Int64("commit-offset", req.CommitOffset).
		Int64("offset", req.Entry.Offset).
		Msg("Add entry")

	// A follower node confirms an entry to the leader
	//
	// The follower adds the entry to its log, sets the head offset
	// and updates its commit offset with the commit offset of
	// the request.
	fc.status = proto.ServingStatus_Follower

	if req.Entry.Offset <= fc.lastAppendedOffset {
		// This was a duplicated request. We already have this entry
		fc.log.Debug().
			Int64("commit-offset", req.CommitOffset).
			Int64("offset", req.Entry.Offset).
			Msg("Ignoring duplicated entry")
		if err := stream.Send(&proto.Ack{Offset: req.Entry.Offset}); err != nil {
			fc.closeStreamNoMutex(err)
		}
		return nil
	}

	// Append the entry asynchronously. We'll sync it in a group from the "sync" routine,
	// where the ack is then sent back
	if err := fc.wal.AppendAsync(req.GetEntry()); err != nil {
		return err
	}

	fc.advertisedCommitOffset.Store(req.CommitOffset)
	fc.lastAppendedOffset = req.Entry.Offset

	// Trigger the sync
	fc.syncCond.Signal()
	return nil
}

func (fc *followerController) handleReplicateSync(stream proto.OxiaLogReplication_ReplicateServer) {
	for {
		fc.Lock()
		if err := fc.syncCond.Wait(stream.Context()); err != nil {
			fc.Unlock()
			fc.closeStream(err)
			return
		}
		fc.Unlock()

		oldHeadOffset := fc.wal.LastOffset()

		if err := fc.wal.Sync(stream.Context()); err != nil {
			fc.closeStream(err)
			return
		}

		// Ack all the entries that were synced in the last round
		newHeadOffset := fc.wal.LastOffset()
		for offset := oldHeadOffset + 1; offset <= newHeadOffset; offset++ {
			if err := stream.Send(&proto.Ack{Offset: offset}); err != nil {
				fc.closeStream(err)
				return
			}
		}

		fc.applyEntriesCond.Signal()
	}
}

func (fc *followerController) applyAllCommittedEntries() {
	for {
		fc.Lock()
		if err := fc.applyEntriesCond.Wait(fc.ctx); err != nil {
			fc.Unlock()
			return
		}
		fc.Unlock()

		maxInclusive := fc.advertisedCommitOffset.Load()
		if err := fc.processCommittedEntries(maxInclusive); err != nil {
			fc.closeStream(err)
			return
		}
	}
}

func (fc *followerController) processCommittedEntries(maxInclusive int64) error {
	fc.log.Debug().
		Int64("min-exclusive", fc.commitOffset.Load()).
		Int64("max-inclusive", maxInclusive).
		Int64("head-offset", fc.wal.LastOffset()).
		Msg("Process committed entries")
	if maxInclusive <= fc.commitOffset.Load() {
		return nil
	}

	reader, err := fc.wal.NewReader(fc.commitOffset.Load())
	if err != nil {
		fc.log.Err(err).Msg("Error opening reader used for applying committed entries")
		return err
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			fc.log.Err(err).Msg("Error closing reader used for applying committed entries")
		}
	}()

	for reader.HasNext() {
		entry, err := reader.ReadNext()

		if err == wal.ErrorReaderClosed {
			fc.log.Info().Msg("Stopped reading committed entries")
			return err
		} else if err != nil {
			fc.log.Err(err).Msg("Error reading committed entry")
			return err
		}

		fc.log.Debug().
			Int64("offset", entry.Offset).
			Msg("Reading entry")

		if entry.Offset > maxInclusive {
			// We read up to the max point
			return nil
		}

		br := &proto.WriteRequest{}
		br.Reset()
		if err := pb.Unmarshal(entry.Value, br); err != nil {
			fc.log.Err(err).Msg("Error unmarshalling committed entry")
			return err
		}

		_, err = fc.db.ProcessWrite(br, entry.Offset, entry.Timestamp, SessionUpdateOperationCallback)
		if err != nil {
			fc.log.Err(err).Msg("Error applying committed entry")
			return err
		}

		fc.commitOffset.Store(entry.Offset)
	}

	return err
}

func GetHighestEntryOfTerm(w wal.Wal, term int64) (*proto.EntryId, error) {
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

type MessageWithTerm interface {
	GetTerm() int64
}

func checkStatus(expected, actual proto.ServingStatus) error {
	if actual != expected {
		return status.Errorf(common.CodeInvalidStatus, "Received message in the wrong state. In %+v, should be %+v.", actual, expected)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////// Handling of snapshots
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (fc *followerController) SendSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error {
	fc.Lock()

	if fc.closeStreamWg != nil {
		fc.Unlock()
		return common.ErrorLeaderAlreadyConnected
	}

	closeStreamWg := common.NewWaitGroup(1)
	fc.closeStreamWg = closeStreamWg
	fc.Unlock()

	go common.DoWithLabels(map[string]string{
		"oxia":  "receive-snapshot",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() { fc.handleSnapshot(stream) })

	return closeStreamWg.Wait(fc.ctx)
}

func (fc *followerController) handleSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) {
	fc.Lock()
	defer fc.Unlock()

	// Wipe out both WAL and DB contents
	err := fc.wal.Clear()
	if err != nil {
		fc.closeStreamNoMutex(err)
		return
	}

	if fc.db != nil {
		err = fc.db.Close()
		if err != nil {
			fc.closeStreamNoMutex(err)
			return
		}

		fc.db = nil
	}

	loader, err := fc.kvFactory.NewSnapshotLoader(fc.shardId)
	if err != nil {
		fc.closeStreamNoMutex(err)
		return
	}

	defer loader.Close()

	var totalSize int64

	for {
		snapChunk, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fc.closeStreamNoMutex(err)
			return
		} else if snapChunk == nil {
			break
		} else if fc.term != wal.InvalidTerm && snapChunk.Term != fc.term {
			// The follower could be left with term=-1 by a previous failed
			// attempt at sending the snapshot. It's ok to proceed in that case.
			fc.closeStreamNoMutex(common.ErrorInvalidTerm)
			return
		}

		fc.term = snapChunk.Term

		fc.log.Debug().
			Str("chunk-name", snapChunk.Name).
			Int("chunk-size", len(snapChunk.Content)).
			Str("chunk-progress", fmt.Sprintf("%d/%d", snapChunk.ChunkIndex, snapChunk.ChunkCount)).
			Int64("term", fc.term).
			Msg("Applying snapshot chunk")
		if err = loader.AddChunk(snapChunk.Name, snapChunk.ChunkIndex, snapChunk.ChunkCount, snapChunk.Content); err != nil {
			fc.closeStream(err)
			return
		}

		totalSize += int64(len(snapChunk.Content))
	}

	// We have received all the files for the database
	loader.Complete()

	newDb, err := kv.NewDB(fc.shardId, fc.kvFactory, fc.config.NotificationsRetentionTime, common.SystemClock)
	if err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "failed to open database after loading snapshot"))
		return
	}

	// The new term must be persisted, to avoid rolling it back
	if err = newDb.UpdateTerm(fc.term); err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "Failed to update term in db"))
	}

	commitOffset, err := newDb.ReadCommitOffset()
	if err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "Failed to read committed offset in the new snapshot"))
		return
	}

	if err = stream.SendAndClose(&proto.SnapshotResponse{
		AckOffset: commitOffset,
	}); err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "Failed to send response after processing snapshot"))
		return
	}

	fc.db = newDb
	fc.commitOffset.Store(commitOffset)
	fc.closeStreamNoMutex(nil)

	fc.log.Info().
		Int64("term", fc.term).
		Int64("snapshot-size", totalSize).
		Int64("commit-offset", commitOffset).
		Msg("Successfully applied snapshot")
}

func (fc *followerController) GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	return &proto.GetStatusResponse{
		Term:   fc.term,
		Status: fc.status,
	}, nil
}
