package server

import (
	"context"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
	"time"
)

func AssertProtoEqual(t *testing.T, expected, actual pb.Message) {
	if !pb.Equal(expected, actual) {
		protoMarshal := protojson.MarshalOptions{
			EmitUnpopulated: true,
		}
		expectedJson, _ := protoMarshal.Marshal(expected)
		actualJson, _ := protoMarshal.Marshal(actual)
		assert.Equal(t, string(expectedJson), string(actualJson))
	}
}

func TestLeaderController_NotInitialized(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_NotMember, lc.Status())

	res, err := lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("value-a")}},
	})

	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	res2, err := lc.Read(&proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a"}},
	})

	assert.Nil(t, res2)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_NoFencing(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_NotMember, lc.Status())
	resp, err := lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, CodeInvalidStatus, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF1(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_NotMember, lc.Status())

	fr, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   1,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, InvalidEntryId, fr.HeadIndex)

	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Leader, lc.Status())

	/// Write entry
	res, err := lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("value-a")}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Puts))
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.EqualValues(t, 0, res.Puts[0].Stat.Version)

	/// Read entry
	res2, err := lc.Read(&proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a", IncludePayload: true}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res2.Gets))
	assert.Equal(t, proto.Status_OK, res2.Gets[0].Status)
	assert.Equal(t, []byte("value-a"), res2.Gets[0].Payload)
	assert.EqualValues(t, 0, res.Puts[0].Stat.Version)

	/// Fence leader

	fr2, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Epoch: 1, Offset: 0}, fr2.HeadIndex)

	assert.EqualValues(t, 2, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	// Should not accept anymore writes & reads

	res3, err := lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("value-a")}},
	})

	assert.Nil(t, res3)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	res4, err := lc.Read(&proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a"}},
	})

	assert.Nil(t, res4)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF2(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	rpc := newMockRpcClient()

	lc, err := NewLeaderController(Config{}, shard, rpc, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_NotMember, lc.Status())

	fr, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   1,
	})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fr.HeadIndex)

	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Leader, lc.Status())

	go func() {
		req := <-rpc.addEntryReqs

		rpc.addEntryResps <- &proto.AddEntryResponse{
			Offset: req.Entry.Offset,
		}
	}()

	/// Write entry
	res, err := lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("value-a")}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Puts))
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.EqualValues(t, 0, res.Puts[0].Stat.Version)

	/// Read entry
	res2, err := lc.Read(&proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a", IncludePayload: true}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res2.Gets))
	assert.Equal(t, proto.Status_OK, res2.Gets[0].Status)
	assert.Equal(t, []byte("value-a"), res2.Gets[0].Payload)
	assert.EqualValues(t, 0, res.Puts[0].Stat.Version)

	/// Fence leader

	fr2, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Epoch: 1, Offset: 0}, fr2.HeadIndex)

	assert.EqualValues(t, 2, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	// Should not accept anymore writes & reads

	res3, err := lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("value-a")}},
	})

	assert.Nil(t, res3)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	res4, err := lc.Read(&proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a"}},
	})

	assert.Nil(t, res4)
	assert.ErrorIs(t, err, ErrorInvalidStatus)

	close(rpc.addEntryResps)
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_EpochPersistent(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_NotMember, lc.Status())

	/// Fence leader

	fr2, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   5,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Epoch: wal.InvalidEpoch, Offset: wal.InvalidOffset}, fr2.HeadIndex)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	assert.NoError(t, lc.Close())

	/// Re-Open lead controller
	lc, err = NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())
	assert.NoError(t, lc.Close())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_FenceEpoch(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	db, err := kv.NewDB(shard, kvFactory)
	assert.NoError(t, err)
	// Force a new epoch in the DB before opening
	assert.NoError(t, db.UpdateEpoch(5))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	// Smaller epoch will fail
	fr, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   4,
	})
	assert.Nil(t, fr)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	// Same epoch will succeed
	fr, err = lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   5,
	})
	assert.NoError(t, err)
	assert.NotNil(t, fr)
	AssertProtoEqual(t, InvalidEntryId, fr.HeadIndex)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeaderEpoch(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	db, err := kv.NewDB(shard, kvFactory)
	assert.NoError(t, err)
	// Force a new epoch in the DB before opening
	assert.NoError(t, db.UpdateEpoch(5))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	// Smaller epoch will fail
	resp, err := lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             4,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	// Higher epoch will fail
	resp, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             6,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	// Same epoch will succeed
	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             5,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollower(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.Fence(&proto.FenceRequest{
		Epoch:   5,
		ShardId: shard,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, proto.ServingStatus_Fenced, lc.Status())

	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	// f1 is already connected
	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:           shard,
		Epoch:             5,
		FollowerName:      "f1",
		FollowerHeadIndex: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Error(t, err)

	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:           shard,
		Epoch:             5,
		FollowerName:      "f2",
		FollowerHeadIndex: InvalidEntryId,
	})
	assert.NoError(t, err)

	// We have already 2 followers and with replication-factor=3
	// it's not possible to add any more followers
	afRes, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:           shard,
		Epoch:             5,
		FollowerName:      "f3",
		FollowerHeadIndex: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Error(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// When a follower is added after the initial leader election,
// the leader should use the head-index at the time of the
// election instead of the current head-index.
// Also, it should never ask to truncate to an offset higher
// than what the follower has.
func TestLeaderController_AddFollower_Truncate(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	// Prepare some data in the leader log & db
	wal, err := walFactory.NewWal(shard)
	assert.NoError(t, err)
	db, err := kv.NewDB(shard, kvFactory)
	assert.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		assert.NoError(t, wal.Append(&proto.LogEntry{
			Epoch:  5,
			Offset: i,
			Value:  []byte(""), // Log content entries are not important for this test
		}))

		_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:     "my-key",
			Payload: []byte(""),
		}}}, i-1, 0)
		assert.NoError(t, err)
	}

	assert.NoError(t, db.UpdateEpoch(5))
	assert.NoError(t, db.Close())
	assert.NoError(t, wal.Close())

	rpcClient := newMockRpcClient()

	lc, err := NewLeaderController(Config{}, shard, rpcClient, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.Fence(&proto.FenceRequest{
		Epoch:   6,
		ShardId: shard,
	})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             6,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": {Epoch: 5, Offset: 9},
		},
	})
	assert.NoError(t, err)

	/// Add some entries in epoch 6 that will be only in the
	/// leader and f1
	go func() {
		for i := 0; i < 10; i++ {
			req := <-rpcClient.addEntryReqs

			rpcClient.addEntryResps <- &proto.AddEntryResponse{
				Offset: req.Entry.Offset,
			}
		}
	}()

	/// Write entries
	for i := 10; i < 20; i++ {
		res, err := lc.Write(&proto.WriteRequest{
			ShardId: &shard,
			Puts: []*proto.PutRequest{{
				Key:     "my-key",
				Payload: []byte("")}},
		})

		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(res.Puts))
		assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
		assert.EqualValues(t, i, res.Puts[0].Stat.Version)
	}

	rpcClient.truncateResps <- struct {
		*proto.TruncateResponse
		error
	}{&proto.TruncateResponse{HeadIndex: &proto.EntryId{Epoch: 5, Offset: 9}}, nil}

	// Adding a follower that needs to be truncated
	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:      shard,
		Epoch:        6,
		FollowerName: "f2",
		FollowerHeadIndex: &proto.EntryId{
			Epoch:  5,
			Offset: 12,
		},
	})
	assert.NoError(t, err)

	trReq := <-rpcClient.truncateReqs
	assert.EqualValues(t, 6, trReq.Epoch)
	AssertProtoEqual(t, &proto.EntryId{Epoch: 5, Offset: 9}, trReq.HeadIndex)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollowerCheckEpoch(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.Fence(&proto.FenceRequest{
		Epoch:   5,
		ShardId: shard,
	})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:           shard,
		Epoch:             4,
		FollowerName:      "f2",
		FollowerHeadIndex: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	afRes, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:           shard,
		Epoch:             6,
		FollowerName:      "f2",
		FollowerHeadIndex: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// When a leader starts, before we can start to serve write/read requests, we need to ensure
// that all the entries that are in the leader wal are fully committed and applied into the db.
// Otherwise, we could have the scenario where entries were already acked to a client though
// are not appearing when doing a subsequent read if the leader has changed.
func TestLeaderController_EntryVisibilityAfterBecomingLeader(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	wal, err := walFactory.NewWal(shard)
	assert.NoError(t, err)
	v, err := pb.Marshal(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "my-key",
			Payload: []byte("my-value"),
		}},
	})
	assert.NoError(t, err)
	assert.NoError(t, wal.Append(&proto.LogEntry{
		Epoch:  0,
		Offset: 0,
		Value:  v,
	}))

	rpc := newMockRpcClient()

	lc, _ := NewLeaderController(Config{}, shard, rpc, walFactory, kvFactory)

	_, _ = lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   1,
	})

	// Respond to replication flow to follower
	go func() {
		req := <-rpc.addEntryReqs

		rpc.addEntryResps <- &proto.AddEntryResponse{
			Offset: req.Entry.Offset,
		}
	}()

	_, _ = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{
			// The follower does not have the entry in its wal yet
			"f1": {Epoch: 0, Offset: -1},
		},
	})

	/// We should be able to read the entry, even if it was not fully committed before the leader started
	res, err := lc.Read(&proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "my-key", IncludePayload: true}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Gets))
	assert.Equal(t, proto.Status_OK, res.Gets[0].Status)
	assert.Equal(t, []byte("my-value"), res.Gets[0].Payload)
	assert.EqualValues(t, 0, res.Gets[0].Stat.Version)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_Notifications(t *testing.T) {
	var shard uint32 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, _ := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.Fence(&proto.FenceRequest{ShardId: shard, Epoch: 1})
	_, _ = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	ctx, cancel := context.WithCancel(context.Background())
	stream := newMockGetNotificationsServer(ctx)

	closeCh := make(chan any)

	go func() {
		err := lc.GetNotifications(&proto.NotificationsRequest{ShardId: shard, StartOffsetExclusive: &wal.InvalidOffset}, stream)
		assert.ErrorIs(t, err, context.Canceled)
		close(closeCh)
	}()

	/// Write entry
	_, _ = lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("value-a")}},
	})

	nb1 := <-stream.ch
	assert.EqualValues(t, 0, nb1.Offset)
	assert.Equal(t, 1, len(nb1.Notifications))
	n1 := nb1.Notifications["a"]
	assert.Equal(t, proto.NotificationType_KeyCreated, n1.Type)
	assert.EqualValues(t, 0, *n1.Version)

	// The handler is still running waiting for more notifications
	select {
	case <-closeCh:
		assert.Fail(t, "Shouldn't have been terminated")

	case <-time.After(1 * time.Second):
		// Expected to timeout
	}

	// Cancelling the stream context should close the `GetNotification()` handler
	cancel()

	select {
	case <-closeCh:
		// Expected to be already closed

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_NotificationsCloseLeader(t *testing.T) {
	var shard uint32 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, _ := NewLeaderController(Config{}, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.Fence(&proto.FenceRequest{ShardId: shard, Epoch: 1})
	_, _ = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	stream := newMockGetNotificationsServer(context.Background())

	closeCh := make(chan any)

	go func() {
		err := lc.GetNotifications(&proto.NotificationsRequest{ShardId: shard, StartOffsetExclusive: &wal.InvalidOffset}, stream)
		assert.ErrorIs(t, err, context.Canceled)
		close(closeCh)
	}()

	// The handler is still running waiting for more notifications
	select {
	case <-closeCh:
		assert.Fail(t, "Shouldn't have been terminated")

	case <-time.After(1 * time.Second):
		// Expected to timeout
	}

	// Closing the leader should close the `GetNotification()` handler
	assert.NoError(t, lc.Close())

	select {
	case <-closeCh:
		// Expected to be already closed

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
