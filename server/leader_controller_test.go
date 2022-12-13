package server

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
)

func TestLeaderController_NotInitialized(t *testing.T) {
	var shard uint32 = 1

	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())

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

	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())
	resp, err := lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF1(t *testing.T) {
	var shard uint32 = 1

	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())

	fr, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   1,
	})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fr.HeadIndex)

	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Epoch())
	assert.Equal(t, Leader, lc.Status())

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
	assert.Equal(t, &proto.EntryId{Epoch: 1, Offset: 0}, fr2.HeadIndex)

	assert.EqualValues(t, 2, lc.Epoch())
	assert.Equal(t, Fenced, lc.Status())

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

	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	rpc := newMockRpcClient()

	lc, err := NewLeaderController(shard, rpc, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())

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
	assert.Equal(t, Leader, lc.Status())

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
	assert.Equal(t, &proto.EntryId{Epoch: 1, Offset: 0}, fr2.HeadIndex)

	assert.EqualValues(t, 2, lc.Epoch())
	assert.Equal(t, Fenced, lc.Status())

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

	kvFactory := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidEpoch, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())

	/// Fence leader

	fr2, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   5,
	})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fr2.HeadIndex)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, Fenced, lc.Status())

	assert.NoError(t, lc.Close())

	/// Re-Open lead controller
	lc, err = NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())
	assert.NoError(t, lc.Close())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_FenceEpoch(t *testing.T) {
	var shard uint32 = 1

	kvFactory := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	db, err := kv.NewDB(shard, kvFactory)
	assert.NoError(t, err)
	// Force a new epoch in the DB before opening
	assert.NoError(t, db.UpdateEpoch(5))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())

	// Smaller epoch will fail
	fr, err := lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   4,
	})
	assert.Nil(t, fr)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	// Same epoch will fail
	fr, err = lc.Fence(&proto.FenceRequest{
		ShardId: shard,
		Epoch:   5,
	})
	assert.Nil(t, fr)
	assert.Equal(t, CodeInvalidEpoch, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeaderEpoch(t *testing.T) {
	var shard uint32 = 1

	kvFactory := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	db, err := kv.NewDB(shard, kvFactory)
	assert.NoError(t, err)
	// Force a new epoch in the DB before opening
	assert.NoError(t, db.UpdateEpoch(5))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, NotMember, lc.Status())

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
	resp, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
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

	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.Fence(&proto.FenceRequest{
		Epoch:   5,
		ShardId: shard,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Epoch())
	assert.Equal(t, Fenced, lc.Status())

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

func TestLeaderController_AddFollowerCheckEpoch(t *testing.T) {
	var shard uint32 = 1

	kvFactory := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	lc, err := NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
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
