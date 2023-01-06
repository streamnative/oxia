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
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
)

// FollowerController handles all the operations of a given shard's follower
type FollowerController interface {
	io.Closer

	// Fence
	//
	// Node handles a fence request
	//
	// A node receives a fencing request, fences itself and responds
	// with its head index.
	//
	// When a node is fenced it cannot:
	// - accept any writes from a client.
	// - accept addEntryRequests from a leader.
	// - send any entries to followers if it was a leader.
	//
	// Any existing follow cursors are destroyed as is any state
	//regarding reconfigurations.
	Fence(req *proto.FenceRequest) (*proto.FenceResponse, error)

	// Truncate
	//
	// A node that receives a truncate request knows that it
	// has been selected as a follower. It truncates its log
	// to the indicates entry id, updates its epoch and changes
	// to a Follower.
	Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error)

	AddEntries(stream proto.OxiaLogReplication_AddEntriesServer) error

	SendSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error

	GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error)

	Epoch() int64
	CommitIndex() int64
	Status() proto.ServingStatus
}

type followerController struct {
	sync.Mutex

	shardId     uint32
	epoch       int64
	commitIndex int64
	headIndex   int64
	status      proto.ServingStatus
	wal         wal.Wal
	walTrimmer  wal.Trimmer
	kvFactory   kv.KVFactory
	db          kv.DB

	ctx           context.Context
	cancel        context.CancelFunc
	closeStreamCh chan error
	log           zerolog.Logger
}

func NewFollowerController(config Config, shardId uint32, wf wal.WalFactory, kvFactory kv.KVFactory) (FollowerController, error) {
	fc := &followerController{
		shardId:       shardId,
		kvFactory:     kvFactory,
		status:        proto.ServingStatus_NotMember,
		closeStreamCh: nil,
		log: log.With().
			Str("component", "follower-controller").
			Uint32("shard", shardId).
			Logger(),
	}
	fc.ctx, fc.cancel = context.WithCancel(context.Background())

	var err error
	if fc.wal, err = wf.NewWal(shardId); err != nil {
		return nil, err
	}

	fc.walTrimmer = wal.NewTrimmer(shardId, fc.wal, config.WalRetentionTime, wal.DefaultCheckInterval, common.SystemClock)

	if fc.db, err = kv.NewDB(shardId, kvFactory); err != nil {
		return nil, err
	}

	entryId, err := GetHighestEntryOfEpoch(fc.wal, MaxEpoch)
	if err != nil {
		return nil, err
	}
	fc.headIndex = entryId.Offset

	if fc.epoch, err = fc.db.ReadEpoch(); err != nil {
		return nil, err
	}

	if fc.epoch != wal.InvalidEpoch {
		fc.status = proto.ServingStatus_Fenced
	}

	if fc.commitIndex, err = fc.db.ReadCommitIndex(); err != nil {
		return nil, err
	}

	fc.log = fc.log.With().Int64("epoch", fc.epoch).Logger()

	fc.log.Info().
		Int64("head-index", fc.headIndex).
		Int64("commit-index", fc.commitIndex).
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

func (fc *followerController) closeChannel(err error) {
	fc.Lock()
	defer fc.Unlock()

	fc.closeChannelNoMutex(err)
}

func (fc *followerController) closeChannelNoMutex(err error) {
	if err != nil && err != io.EOF && status.Code(err) != codes.Canceled {
		fc.log.Warn().Err(err).
			Msg("Error in handle AddEntries stream")
	}

	if fc.closeStreamCh != nil {
		select {
		case fc.closeStreamCh <- err:
		default:
			// Only write if there's a listener
		}
		close(fc.closeStreamCh)
		fc.closeStreamCh = nil
	}
}

func (fc *followerController) Status() proto.ServingStatus {
	fc.Lock()
	defer fc.Unlock()
	return fc.status
}

func (fc *followerController) Epoch() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.epoch
}

func (fc *followerController) CommitIndex() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.commitIndex
}

func (fc *followerController) Fence(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if req.Epoch < fc.epoch {
		fc.log.Warn().
			Int64("follower-epoch", fc.epoch).
			Int64("fence-epoch", req.Epoch).
			Msg("Failed to fence with invalid epoch")
		return nil, common.ErrorInvalidEpoch
	} else if req.Epoch == fc.epoch && fc.status != proto.ServingStatus_Fenced {
		// It's OK to receive a duplicate Fence request, for the same epoch, as long as we haven't moved
		// out of the Fenced state for that epoch
		fc.log.Warn().
			Int64("follower-epoch", fc.epoch).
			Int64("fence-epoch", req.Epoch).
			Interface("status", fc.status).
			Msg("Failed to fence with same epoch in invalid state")
		return nil, common.ErrorInvalidStatus
	}

	if err := fc.db.UpdateEpoch(req.Epoch); err != nil {
		return nil, err
	}

	fc.epoch = req.Epoch
	fc.log = fc.log.With().Int64("epoch", fc.epoch).Logger()
	fc.status = proto.ServingStatus_Fenced
	fc.closeChannelNoMutex(nil)

	lastEntryId, err := getLastEntryIdInWal(fc.wal)
	if err != nil {
		fc.log.Warn().Err(err).
			Int64("follower-epoch", fc.epoch).
			Int64("fence-epoch", req.Epoch).
			Msg("Failed to get last")
		return nil, err
	}

	fc.log.Info().
		Interface("last-entry", lastEntryId).
		Msg("Follower successfully fenced")
	return &proto.FenceResponse{HeadIndex: lastEntryId}, nil
}

func (fc *followerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if fc.status != proto.ServingStatus_Fenced {
		return nil, common.ErrorInvalidStatus
	}

	if req.Epoch != fc.epoch {
		return nil, common.ErrorInvalidEpoch
	}

	fc.status = proto.ServingStatus_Follower
	headIndex, err := fc.wal.TruncateLog(req.HeadIndex.Offset)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to truncate wal. truncate-offset: %d - wal-last-offset: %d",
			req.HeadIndex.Offset, fc.wal.LastOffset())
	}

	fc.headIndex = headIndex

	return &proto.TruncateResponse{
		HeadIndex: &proto.EntryId{
			Epoch:  req.Epoch,
			Offset: headIndex,
		},
	}, nil
}

func (fc *followerController) AddEntries(stream proto.OxiaLogReplication_AddEntriesServer) error {
	fc.Lock()
	if fc.status != proto.ServingStatus_Fenced && fc.status != proto.ServingStatus_Follower {
		return common.ErrorInvalidStatus
	}

	if fc.closeStreamCh != nil {
		fc.Unlock()
		return common.ErrorLeaderAlreadyConnected
	}

	closeStreamCh := make(chan error)
	fc.closeStreamCh = closeStreamCh
	fc.Unlock()

	go common.DoWithLabels(map[string]string{
		"oxia":  "receive-shards-assignments",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() { fc.handleServerStream(stream) })

	select {
	case err := <-closeStreamCh:
		return err
	case <-fc.ctx.Done():
		return nil
	}
}

func (fc *followerController) handleServerStream(stream proto.OxiaLogReplication_AddEntriesServer) {
	for {
		if addEntryReq, err := stream.Recv(); err != nil {
			fc.closeChannel(err)
			return
		} else if addEntryReq == nil {
			fc.closeChannel(nil)
			return
		} else if res, err := fc.addEntry(addEntryReq); err != nil {
			fc.closeChannel(err)
			return
		} else if err = stream.Send(res); err != nil {
			fc.closeChannel(err)
			return
		}
	}
}

func (fc *followerController) addEntry(req *proto.AddEntryRequest) (*proto.AddEntryResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if req.Epoch != fc.epoch {
		return nil, common.ErrorInvalidEpoch
	}

	fc.log.Debug().
		Int64("commit-index", req.CommitIndex).
		Int64("offset", req.Entry.Offset).
		Msg("Add entry")

	// A follower node confirms an entry to the leader
	//
	// The follower adds the entry to its log, sets the head index
	// and updates its commit index with the commit index of
	// the request.
	fc.status = proto.ServingStatus_Follower

	if req.Entry.Offset <= fc.headIndex {
		// This was a duplicated request. We already have this entry
		fc.log.Debug().
			Int64("commit-index", req.CommitIndex).
			Int64("offset", req.Entry.Offset).
			Msg("Ignoring duplicated entry")
		return &proto.AddEntryResponse{Offset: req.Entry.Offset}, nil
	}

	if err := fc.wal.Append(req.GetEntry()); err != nil {
		return nil, err
	}

	fc.headIndex = req.Entry.Offset
	if err := fc.processCommittedEntries(req.CommitIndex); err != nil {
		return nil, err
	}
	return &proto.AddEntryResponse{
		Offset: req.Entry.Offset,
	}, nil

}

func (fc *followerController) processCommittedEntries(maxInclusive int64) error {
	fc.log.Debug().
		Int64("min-exclusive", fc.commitIndex).
		Int64("max-inclusive", maxInclusive).
		Int64("head-index", fc.headIndex).
		Msg("Process committed entries")
	if maxInclusive <= fc.commitIndex {
		return nil
	}

	reader, err := fc.wal.NewReader(fc.commitIndex)
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
		if err := pb.Unmarshal(entry.Value, br); err != nil {
			fc.log.Err(err).Msg("Error unmarshalling committed entry")
			return err
		}

		_, err = fc.db.ProcessWrite(br, entry.Offset, entry.Timestamp, SessionUpdateOperationCallback)
		if err != nil {
			fc.log.Err(err).Msg("Error applying committed entry")
			return err
		}

		fc.commitIndex = entry.Offset
	}
	return err
}

func GetHighestEntryOfEpoch(w wal.Wal, epoch int64) (*proto.EntryId, error) {
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
		if e.Epoch <= epoch {
			return &proto.EntryId{
				Epoch:  e.Epoch,
				Offset: e.Offset,
			}, nil
		}
	}
	return InvalidEntryId, nil
}

type MessageWithEpoch interface {
	GetEpoch() int64
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

	if fc.closeStreamCh != nil {
		fc.Unlock()
		return common.ErrorLeaderAlreadyConnected
	}

	closeStreamCh := make(chan error)
	fc.closeStreamCh = closeStreamCh
	fc.Unlock()

	go common.DoWithLabels(map[string]string{
		"oxia":  "receive-snapshot",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() { fc.handleSnapshot(stream) })

	select {
	case err := <-closeStreamCh:
		return err
	case <-fc.ctx.Done():
		return fc.ctx.Err()
	}
}

func (fc *followerController) handleSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) {
	fc.Lock()
	defer fc.Unlock()

	// Wipe out both WAL and DB contents
	err := fc.wal.Clear()
	if err != nil {
		fc.closeChannelNoMutex(err)
		return
	}

	if fc.db != nil {
		err = fc.db.Close()
		if err != nil {
			fc.closeChannelNoMutex(err)
			return
		}

		fc.db = nil
	}

	loader, err := fc.kvFactory.NewSnapshotLoader(fc.shardId)
	if err != nil {
		fc.closeChannelNoMutex(err)
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
			fc.closeChannelNoMutex(err)
			return
		} else if snapChunk == nil {
			break
		} else if fc.epoch != wal.InvalidEpoch && snapChunk.Epoch != fc.epoch {
			// The follower could be left with epoch=-1 by a previous failed
			// attempt at sending the snapshot. It's ok to proceed in that case.
			fc.closeChannelNoMutex(common.ErrorInvalidEpoch)
			return
		}

		fc.epoch = snapChunk.Epoch

		fc.log.Debug().
			Str("chunk-name", snapChunk.Name).
			Int("chunk-size", len(snapChunk.Content)).
			Int64("epoch", fc.epoch).
			Msg("Applying snapshot chunk")
		if err = loader.AddChunk(snapChunk.Name, snapChunk.Content); err != nil {
			fc.closeChannel(err)
			return
		}

		totalSize += int64(len(snapChunk.Content))
	}

	// We have received all the files for the database
	loader.Complete()

	newDb, err := kv.NewDB(fc.shardId, fc.kvFactory)
	if err != nil {
		fc.closeChannelNoMutex(errors.Wrap(err, "failed to open database after loading snapshot"))
		return
	}

	// The new epoch must be persisted, to avoid rolling it back
	if err = newDb.UpdateEpoch(fc.epoch); err != nil {
		fc.closeChannelNoMutex(errors.Wrap(err, "Failed to update epoch in db"))
	}

	commitIndex, err := newDb.ReadCommitIndex()
	if err != nil {
		fc.closeChannelNoMutex(errors.Wrap(err, "Failed to read committed index in the new snapshot"))
		return
	}

	if err = stream.SendAndClose(&proto.SnapshotResponse{
		AckIndex: commitIndex,
	}); err != nil {
		fc.closeChannelNoMutex(errors.Wrap(err, "Failed to send response after processing snapshot"))
		return
	}

	fc.db = newDb
	fc.commitIndex = commitIndex
	fc.closeChannelNoMutex(nil)

	fc.log.Info().
		Int64("epoch", fc.epoch).
		Int64("snapshot-size", totalSize).
		Int64("commit-index", commitIndex).
		Msg("Successfully applied snapshot")
}

func (fc *followerController) GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	return &proto.GetStatusResponse{
		Epoch:  fc.epoch,
		Status: fc.status,
	}, nil
}
