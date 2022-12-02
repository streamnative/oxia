package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	pb "google.golang.org/protobuf/proto"
	"io"
	"math"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
)

const MaxEpoch = math.MaxInt64

var ErrorInvalidEpoch = errors.New("oxia: invalid epoch")
var ErrorInvalidStatus = errors.New("oxia: invalid status")

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

	Epoch() int64
	Status() Status
}

type followerController struct {
	sync.Mutex

	shardId     uint32
	epoch       int64
	commitIndex int64
	headIndex   int64
	status      Status
	wal         wal.Wal
	db          kv.DB
	closing     bool
	log         zerolog.Logger
}

func NewFollowerController(shardId uint32, wf wal.WalFactory, kvFactory kv.KVFactory) (FollowerController, error) {
	fc := &followerController{
		shardId:     shardId,
		commitIndex: wal.InvalidOffset,
		headIndex:   wal.InvalidOffset,
		status:      NotMember,
		closing:     false,
		log: log.With().
			Str("component", "follower-controller").
			Uint32("shard", shardId).
			Logger(),
	}

	if wal, err := wf.NewWal(shardId); err != nil {
		return nil, err
	} else {
		fc.wal = wal
	}

	if db, err := kv.NewDB(shardId, kvFactory); err != nil {
		return nil, err
	} else {
		fc.db = db
	}

	entryId, err := GetHighestEntryOfEpoch(fc.wal, MaxEpoch)
	if err != nil {
		return nil, err
	}
	fc.headIndex = entryId.Offset

	if fc.epoch, err = fc.db.ReadEpoch(); err != nil {
		return nil, err
	}

	fc.log = fc.log.With().Int64("epoch", fc.epoch).Logger()

	fc.log.Info().
		Int64("head-index", fc.headIndex).
		Msg("Created follower")
	return fc, nil
}

func (fc *followerController) Close() error {
	if err := fc.wal.Close(); err != nil {
		return err
	}

	if err := fc.db.Close(); err != nil {
		return err
	}

	fc.log.Info().Msg("Closed follower")
	return nil
}

func (fc *followerController) Status() Status {
	fc.Lock()
	defer fc.Unlock()
	return fc.status
}

func (fc *followerController) Epoch() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.epoch
}

func (fc *followerController) Fence(req *proto.FenceRequest) (*proto.FenceResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if err := checkEpochLaterIn(req, fc.epoch); err != nil {
		fc.log.Warn().Err(err).
			Int64("follower-epoch", fc.epoch).
			Int64("fence-epoch", req.Epoch).
			Msg("Failed to fence with invalid epoch")
		return nil, err
	}

	if err := fc.db.UpdateEpoch(req.Epoch); err != nil {
		return nil, err
	}

	fc.epoch = req.Epoch
	fc.log = fc.log.With().Int64("epoch", fc.epoch).Logger()
	fc.status = Fenced

	lastEntryId, err := getLastEntryIdInWal(fc.wal)
	if err != nil {
		fc.log.Warn().Err(err).
			Int64("follower-epoch", fc.epoch).
			Int64("fence-epoch", req.Epoch).
			Msg("Failed to get last")
		return nil, err
	}

	fc.log.Info().
		Msg("Follower successfully fenced")
	return &proto.FenceResponse{
		Epoch:     fc.epoch,
		HeadIndex: lastEntryId,
	}, nil
}

func (fc *followerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if err := checkStatus(Fenced, fc.status); err != nil {
		return nil, err
	}
	if err := checkEpochEqualIn(req, fc.epoch); err != nil {
		return nil, err
	}

	fc.status = Follower
	headIndex, err := fc.wal.TruncateLog(req.HeadIndex.Offset)
	if err != nil {
		return nil, err
	}

	oldCommitIndex, err := fc.db.ReadCommitIndex()
	if err != nil {
		return nil, err
	}

	if err = fc.processCommittedEntries(oldCommitIndex, fc.headIndex); err != nil {
		return nil, err
	}
	fc.headIndex = headIndex
	fc.commitIndex = headIndex

	return &proto.TruncateResponse{
		Epoch: req.Epoch,
		HeadIndex: &proto.EntryId{
			Epoch:  req.Epoch,
			Offset: headIndex,
		},
	}, nil
}

func (fc *followerController) AddEntries(stream proto.OxiaLogReplication_AddEntriesServer) error {
	for {
		if addEntryReq, err := stream.Recv(); err != nil {
			return err
		} else if res, err := fc.addEntry(addEntryReq); err != nil {
			return err
		} else if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (fc *followerController) addEntry(req *proto.AddEntryRequest) (*proto.AddEntryResponse, error) {
	fc.Lock()
	defer fc.Unlock()
	if fc.status != Follower && fc.status != Fenced {
		return nil, errors.Wrapf(ErrorInvalidStatus, "AddEntry request when status = %+v", fc.status)
	}
	if req.Epoch < fc.epoch {
		/*
		 A follower node rejects an entry from the leader.


		  If the leader has a lower epoch than the follower then the
		  follower must reject it with an INVALID_EPOCH response.

		  Key points:
		  - The epoch of the response should be the epoch of the
		    request so that the leader will not ignore the response.
		*/
		return &proto.AddEntryResponse{
			Epoch:        req.Entry.Epoch,
			Offset:       wal.InvalidOffset,
			InvalidEpoch: true,
		}, nil
	}

	// A follower node confirms an entry to the leader
	//
	// The follower adds the entry to its log, sets the head index
	// and updates its commit index with the commit index of
	// the request.
	fc.status = Follower

	if req.Epoch > fc.epoch {
		if err := fc.db.UpdateEpoch(req.Epoch); err != nil {
			return nil, err
		}
		fc.epoch = req.Epoch
		fc.log = fc.log.With().Int64("epoch", fc.epoch).Logger()
		fc.log.Info().
			Msg("Follower upgraded the epoch")
	}

	if err := fc.wal.Append(req.GetEntry()); err != nil {
		return nil, err
	}

	fc.headIndex = req.Entry.Offset
	oldCommitIndex := fc.commitIndex
	fc.commitIndex = req.CommitIndex
	if err := fc.processCommittedEntries(oldCommitIndex, fc.commitIndex); err != nil {
		return nil, err
	}
	return &proto.AddEntryResponse{
		Epoch:        fc.epoch,
		Offset:       req.Entry.Offset,
		InvalidEpoch: false,
	}, nil

}

func (fc *followerController) processCommittedEntries(minExclusive int64, maxInclusive int64) error {
	if maxInclusive <= minExclusive {
		return nil
	}

	reader, err := fc.wal.NewReader(minExclusive)
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

	entry, err := reader.ReadNext()
	for err == nil && entry.Offset <= maxInclusive {
		if err == wal.ErrorReaderClosed {
			fc.log.Info().Msg("Stopped reading committed entries")
			return err
		} else if err != nil {
			fc.log.Err(err).Msg("Error reading committed entry")
			return err
		}

		br := &proto.WriteRequest{}
		if err := pb.Unmarshal(entry.Value, br); err != nil {
			fc.log.Err(err).Msg("Error unmarshalling committed entry")
			return err
		}

		_, err = fc.db.ProcessWrite(br, entry.Offset)
		if err != nil {
			fc.log.Err(err).Msg("Error applying committed entry")
			return err
		}
		entry, err = reader.ReadNext()
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

func checkEpochLaterIn(req MessageWithEpoch, expected int64) error {
	if req.GetEpoch() <= expected {
		return errors.Wrapf(ErrorInvalidEpoch, "Got old epoch %d, when at %d", req.GetEpoch(), expected)
	}
	return nil
}

func checkEpochEqualIn(req MessageWithEpoch, expected int64) error {
	if req.GetEpoch() != expected {
		return errors.Wrapf(ErrorInvalidEpoch, "Got clashing epoch %d, when at %d", req.GetEpoch(), expected)
	}
	return nil
}

func checkStatus(expected, actual Status) error {
	if actual != expected {
		return errors.Wrapf(ErrorInvalidStatus, "Received message in the wrong state. In %+v, should be %+v.", actual, expected)
	}
	return nil
}
