package server

import (
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
)

var testKVOptions = &kv.KVFactoryOptions{
	InMemory:  true,
	CacheSize: 10 * 1024,
}

func TestFollower(t *testing.T) {
	var shardId uint32
	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := NewInMemoryWalFactory()

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, NotMember, fc.Status())

	fenceRes, err := fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, fenceRes.Epoch)
	assert.Equal(t, &proto.EntryId{Epoch: 0, Offset: 0}, fenceRes.HeadIndex)

	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Epoch: 1,
		HeadIndex: &proto.EntryId{
			Epoch:  0,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, truncateResp.Epoch)
	assert.Equal(t, wal.EntryIdFromProto(&proto.EntryId{
		Epoch:  0,
		Offset: 0,
	}), wal.EntryIdFromProto(truncateResp.HeadIndex))

	assert.Equal(t, Follower, fc.Status())

	stream := newMockServerAddEntriesStream()

	go func() { assert.NoError(t, fc.AddEntries(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, &proto.EntryId{
		Epoch:  0,
		Offset: 0,
	}))

	// Wait for response
	response := stream.GetResponse()

	assert.Equal(t, Follower, fc.Status())

	assert.EqualValues(t, 1, response.Epoch)
	assert.Equal(t, wal.EntryIdFromProto(&proto.EntryId{
		Epoch:  1,
		Offset: 0,
	}), wal.EntryIdFromProto(response.EntryId))
	assert.False(t, response.InvalidEpoch)

	// Try to add entry with lower epoch
	stream.AddRequest(createAddRequest(t, 0, 0, map[string]string{"a": "2", "b": "3"}, &proto.EntryId{
		Epoch:  0,
		Offset: 0,
	}))

	// Wait for response
	response = stream.GetResponse()
	assert.EqualValues(t, 0, response.Epoch)
	assert.Nil(t, response.EntryId)
	assert.True(t, response.InvalidEpoch)

	assert.Equal(t, Follower, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	// Try to add entry with higher epoch. This should succeed
	stream.AddRequest(createAddRequest(t, 3, 0, map[string]string{"a": "4", "b": "5"}, &proto.EntryId{
		Epoch:  0,
		Offset: 0,
	}))

	// Wait for response
	response = stream.GetResponse()
	assert.EqualValues(t, 3, response.Epoch)
	assert.Equal(t, wal.EntryIdFromProto(&proto.EntryId{Epoch: 3, Offset: 0}), wal.EntryIdFromProto(response.EntryId))
	assert.False(t, response.InvalidEpoch)

	assert.Equal(t, Follower, fc.Status())
	assert.EqualValues(t, 3, fc.Epoch())

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
	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	// Second fence should fail, because we're already in epoch 1
	fr, err := fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.Nil(t, fr)
	assert.ErrorIs(t, err, ErrorInvalidEpoch)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	_, err = fc.Truncate(&proto.TruncateRequest{
		Epoch: 1,
		HeadIndex: &proto.EntryId{
			Epoch:  0,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, Follower, fc.Status())

	stream := newMockServerAddEntriesStream()
	go func() { assert.NoError(t, fc.AddEntries(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, &proto.EntryId{
		Epoch:  0,
		Offset: 0,
	}))

	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "2", "b": "3"},
		// Commit index points to previous entry
		&proto.EntryId{
			Epoch:  1,
			Offset: 0,
		}))

	// Wait for addEntryResponses
	r1 := stream.GetResponse()

	assert.Equal(t, Follower, fc.Status())

	assert.EqualValues(t, 1, r1.Epoch)
	assert.Equal(t, wal.EntryIdFromProto(&proto.EntryId{
		Epoch:  1,
		Offset: 0,
	}), wal.EntryIdFromProto(r1.EntryId))
	assert.False(t, r1.InvalidEpoch)

	r2 := stream.GetResponse()

	assert.EqualValues(t, 1, r2.Epoch)
	assert.Equal(t, wal.EntryIdFromProto(&proto.EntryId{
		Epoch:  1,
		Offset: 1,
	}), wal.EntryIdFromProto(r2.EntryId))
	assert.False(t, r2.InvalidEpoch)

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

func TestEpochInStateChanges(t *testing.T) {
	var shardId uint32
	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	stream := newMockServerAddEntriesStream()
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, &proto.EntryId{
		Epoch:  0,
		Offset: 0,
	}))

	// Follower will not accept any new entries unless it's in Fenced or Follower states
	err = fc.AddEntries(stream)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	_, err = fc.Fence(&proto.FenceRequest{Epoch: 1})
	assert.NoError(t, err)
	assert.Equal(t, Fenced, fc.Status())
	assert.EqualValues(t, 1, fc.Epoch())

	tr, err := fc.Truncate(&proto.TruncateRequest{
		Epoch: 2,
		HeadIndex: &proto.EntryId{
			Epoch:  0,
			Offset: 0,
		},
	})
	assert.Nil(t, tr)
	assert.ErrorIs(t, err, ErrorInvalidEpoch)
	assert.EqualValues(t, 1, fc.Epoch())
	assert.Equal(t, Fenced, fc.Status())

	_, err = fc.Truncate(&proto.TruncateRequest{
		Epoch: 1,
		HeadIndex: &proto.EntryId{
			Epoch:  0,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, fc.Epoch())
	assert.Equal(t, Follower, fc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestIgnoreInvalidStates(t *testing.T) {
	var shardId uint32
	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
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

func createAddRequest(t *testing.T, epoch uint64, offset uint64,
	kvs map[string]string,
	commitIndex *proto.EntryId) *proto.AddEntryRequest {
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
		EntryId: &proto.EntryId{
			Epoch:  epoch,
			Offset: offset,
		},
		Value: entry,
	}

	return &proto.AddEntryRequest{
		Epoch:       epoch,
		Entry:       le,
		CommitIndex: commitIndex,
	}
}
