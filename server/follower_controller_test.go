package server

import (
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"
	"oxia/common"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
)

var testKVOptions = &kv.KVFactoryOptions{
	InMemory:  true,
	CacheSize: 10 * 1024,
}

func init() {
	common.LogLevel = zerolog.DebugLevel
	common.ConfigureLogger()
}

func TestFollower(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, NotMember, fc.Status())

	fenceRes, err := fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, fenceRes.Epoch)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadIndex)

	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Epoch: 1,
		HeadIndex: &proto.EntryId{
			Epoch:  1,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, truncateResp.Epoch)
	assert.EqualValues(t, 1, truncateResp.HeadIndex.Epoch)
	assert.Equal(t, wal.InvalidOffset, truncateResp.HeadIndex.Offset)

	assert.Equal(t, Follower, fc.Status())

	stream := newMockServerAddEntriesStream()

	go func() { assert.NoError(t, fc.AddEntries(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for response
	response := stream.GetResponse()

	assert.Equal(t, Follower, fc.Status())

	assert.EqualValues(t, 1, response.Epoch)
	assert.EqualValues(t, 0, response.Offset)

	// Write next entry
	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))

	// Wait for response
	response = stream.GetResponse()
	assert.EqualValues(t, 1, response.Epoch)
	assert.EqualValues(t, 1, response.Offset)

	assert.Equal(t, Follower, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	// Double-check the values in the DB
	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:            "a",
		IncludePayload: true}, {
		Key:            "b",
		IncludePayload: true,
	},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	// Keys are not there because they were not part of the commit index
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Gets[0].Status)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Gets[1].Status)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestReadingUpToCommitIndex(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	_, err = fc.Truncate(&proto.TruncateRequest{
		Epoch: 1,
		HeadIndex: &proto.EntryId{
			Epoch:  0,
			Offset: wal.InvalidOffset,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, Follower, fc.Status())

	stream := newMockServerAddEntriesStream()
	go func() { assert.NoError(t, fc.AddEntries(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "2", "b": "3"},
		// Commit index points to previous entry
		0))

	// Wait for addEntryResponses
	r1 := stream.GetResponse()

	assert.Equal(t, Follower, fc.Status())

	assert.EqualValues(t, 1, r1.Epoch)
	assert.EqualValues(t, 0, r1.Offset)

	r2 := stream.GetResponse()

	assert.EqualValues(t, 1, r2.Epoch)
	assert.EqualValues(t, 1, r2.Offset)

	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:            "a",
		IncludePayload: true}, {
		Key:            "b",
		IncludePayload: true,
	},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	// Keys are not there because they were not part of the commit index
	assert.Equal(t, proto.Status_OK, dbRes.Gets[0].Status)
	assert.Equal(t, []byte("0"), dbRes.Gets[0].Payload)
	assert.Equal(t, proto.Status_OK, dbRes.Gets[1].Status)
	assert.Equal(t, []byte("1"), dbRes.Gets[1].Payload)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_FenceEpoch(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	// We cannot fence with earlier epoch
	fr, err := fc.Fence(&proto.FenceRequest{Epoch: 0})
	assert.Nil(t, fr)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	// We cannot fence with same epoch
	fr, err = fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.Nil(t, fr)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	// Higher epoch will work
	_, err = fc.Fence(&proto.FenceRequest{Epoch: 3})
	assert.NoError(t, err)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 3, fc.Epoch())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestIgnoreInvalidStates(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	// Follower needs to be in "Fenced" state to receive a Truncate request
	tr, err := fc.Truncate(&proto.TruncateRequest{
		Epoch: 1,
		HeadIndex: &proto.EntryId{
			Epoch:  0,
			Offset: 0,
		},
	})
	assert.ErrorIs(t, err, ErrorInvalidStatus)
	assert.Nil(t, tr)
	assert.Equal(t, NotMember, fc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_PersistentEpoch(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, NotMember, fc.Status())
	assert.Equal(t, wal.InvalidEpoch, fc.Epoch())

	fenceRes, err := fc.Fence(&proto.FenceRequest{Epoch: 4})
	assert.NoError(t, err)
	assert.EqualValues(t, 4, fenceRes.Epoch)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadIndex)

	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 4, fc.Epoch())

	assert.NoError(t, fc.Close())

	/// Reopen and verify epoch
	fc, err = NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, NotMember, fc.Status())
	assert.EqualValues(t, 4, fc.Epoch())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_CommitIndexLastEntry(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	stream := newMockServerAddEntriesStream()
	go func() { assert.NoError(t, fc.AddEntries(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for addEntryResponses
	r1 := stream.GetResponse()

	assert.Equal(t, Follower, fc.Status())

	assert.EqualValues(t, 1, r1.Epoch)
	assert.EqualValues(t, 0, r1.Offset)

	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:            "a",
		IncludePayload: true}, {
		Key:            "b",
		IncludePayload: true,
	},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	assert.Equal(t, proto.Status_OK, dbRes.Gets[0].Status)
	assert.Equal(t, []byte("0"), dbRes.Gets[0].Payload)
	assert.Equal(t, proto.Status_OK, dbRes.Gets[1].Status)
	assert.Equal(t, []byte("1"), dbRes.Gets[1].Payload)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollowerController_RejectEntriesWithDifferentEpoch(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)

	db, err := kv.NewDB(shardId, kvFactory)
	assert.NoError(t, err)
	// Force a new epoch in the DB before opening
	assert.NoError(t, db.UpdateEpoch(5))
	assert.NoError(t, db.Close())

	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, NotMember, fc.Status())
	assert.EqualValues(t, 5, fc.Epoch())

	stream := newMockServerAddEntriesStream()
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "1", "b": "1"}, wal.InvalidOffset))

	// Follower will reject the entry because it's from an earlier epoch
	err = fc.AddEntries(stream)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, NotMember, fc.Status())
	assert.EqualValues(t, 5, fc.Epoch())

	// If we send an entry of same epoch, it will be accepted
	stream.AddRequest(createAddRequest(t, 5, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))

	go func() { assert.NoError(t, fc.AddEntries(stream)) }()

	// Wait for addEntryResponses
	r1 := stream.GetResponse()

	assert.Equal(t, Follower, fc.Status())
	assert.EqualValues(t, 5, r1.Epoch)
	assert.EqualValues(t, 0, r1.Offset)
	assert.NoError(t, fc.Close())
	close(stream.requests)

	//// A higher epoch will also be rejected
	fc, err = NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	stream = newMockServerAddEntriesStream()
	stream.AddRequest(createAddRequest(t, 6, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))
	err = fc.AddEntries(stream)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, NotMember, fc.Status())
	assert.EqualValues(t, 5, fc.Epoch())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_RejectTruncateInvalidEpoch(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, NotMember, fc.Status())

	fenceRes, err := fc.Fence(&proto.FenceRequest{Epoch: 5})
	assert.NoError(t, err)
	assert.EqualValues(t, 5, fenceRes.Epoch)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadIndex)

	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 5, fc.Epoch())

	// Lower epoch should be rejected
	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Epoch: 4,
		HeadIndex: &proto.EntryId{
			Epoch:  1,
			Offset: 0,
		},
	})
	assert.Nil(t, truncateResp)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 5, fc.Epoch())

	// Truncate with higher epoch should also fail
	truncateResp, err = fc.Truncate(&proto.TruncateRequest{
		Epoch: 6,
		HeadIndex: &proto.EntryId{
			Epoch:  1,
			Offset: 0,
		},
	})
	assert.Nil(t, truncateResp)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 5, fc.Epoch())
}

func createAddRequest(t *testing.T, epoch int64, offset int64,
	kvs map[string]string,
	commitIndex int64) *proto.AddEntryRequest {
	br := &proto.WriteRequest{}

	for k, v := range kvs {
		br.Puts = append(br.Puts, &proto.PutRequest{
			Key:     k,
			Payload: []byte(v),
		})
	}

	entry, err := pb.Marshal(br)
	assert.NoError(t, err)

	le := &proto.LogEntry{
		Epoch:  epoch,
		Offset: offset,
		Value:  entry,
	}

	return &proto.AddEntryRequest{
		Epoch:       epoch,
		Entry:       le,
		CommitIndex: commitIndex,
	}
}
