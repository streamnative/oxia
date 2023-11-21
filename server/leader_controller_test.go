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
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
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
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	res, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.Nil(t, res)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(err))

	r := <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a"}},
	})

	assert.Nil(t, r.Response)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(r.Err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_Closed(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	assert.NoError(t, lc.Close())

	res, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    2,
	})

	assert.Nil(t, res)
	assert.Equal(t, common.CodeAlreadyClosed, status.Code(err))

	res2, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:      shard,
		Term:         2,
		FollowerMaps: map[string]*proto.EntryId{},
	})

	assert.Nil(t, res2)
	assert.Equal(t, common.CodeAlreadyClosed, status.Code(err))

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_NoFencing(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())
	resp, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF1(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	fr, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    1,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, InvalidEntryId, fr.HeadEntryId)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Term())
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	/// Write entry
	res, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Puts))
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	/// Read entry
	r := <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	})

	assert.NoError(t, r.Err)
	assert.Equal(t, proto.Status_OK, r.Response.Status)
	assert.Equal(t, []byte("value-a"), r.Response.Value)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	/// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: 1, Offset: 0}, fr2.HeadEntryId)

	assert.EqualValues(t, 2, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Should not accept anymore writes & reads

	res3, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.Nil(t, res3)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(err))

	r = <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a"}},
	})

	assert.Nil(t, r.Response)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(r.Err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF2(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	rpc := newMockRpcClient()

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, rpc, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	fr, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    1,
	})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fr.HeadEntryId)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Term())
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	go func() {
		req := <-rpc.appendReqs

		rpc.ackResps <- &proto.Ack{
			Offset: req.Entry.Offset,
		}
	}()

	/// Write entry
	res, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Puts))
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	/// Read entry
	r := <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	})

	assert.NoError(t, r.Err)
	assert.Equal(t, proto.Status_OK, r.Response.Status)
	assert.Equal(t, []byte("value-a"), r.Response.Value)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	/// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: 1, Offset: 0}, fr2.HeadEntryId)

	assert.EqualValues(t, 2, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Should not accept anymore writes & reads

	res3, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.Nil(t, res3)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(err))

	r = <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a"}},
	})

	assert.Nil(t, r.Response)
	assert.Equal(t, common.CodeInvalidStatus, status.Code(r.Err))

	close(rpc.ackResps)
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_TermPersistent(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	/// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    5,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: wal.InvalidTerm, Offset: wal.InvalidOffset}, fr2.HeadEntryId)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	assert.NoError(t, lc.Close())

	/// Re-open lead controller
	lc, err = NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())
	assert.NoError(t, lc.Close())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_FenceTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	db, err := kv.NewDB(common.DefaultNamespace, shard, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Smaller term will fail
	fr, err := lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    4,
	})
	assert.Nil(t, fr)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Same term will succeed
	fr, err = lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    5,
	})
	assert.NoError(t, err)
	assert.NotNil(t, fr)
	AssertProtoEqual(t, InvalidEntryId, fr.HeadEntryId)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeaderTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	db, err := kv.NewDB(common.DefaultNamespace, shard, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Smaller term will fail
	resp, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              4,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))

	// Higher term will fail
	resp, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              6,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))

	// Same term will succeed
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              5,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollower(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:    5,
		ShardId: shard,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	// f1 is already connected
	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:             shard,
		Term:                5,
		FollowerName:        "f1",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Error(t, err)

	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:             shard,
		Term:                5,
		FollowerName:        "f2",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.NoError(t, err)

	// We have already 2 followers and with replication-factor=3
	// it's not possible to add any more followers
	afRes, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:             shard,
		Term:                5,
		FollowerName:        "f3",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Error(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// When a follower is added after the initial leader election,
// the leader should use the head-entry at the time of the
// election instead of the current head-entry.
// Also, it should never ask to truncate to an offset higher
// than what the follower has.
func TestLeaderController_AddFollower_Truncate(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{BaseWalDir: t.TempDir()})

	// Prepare some data in the leader log & db
	wal, err := walFactory.NewWal(common.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	db, err := kv.NewDB(common.DefaultNamespace, shard, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		wr := &proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:   "my-key",
			Value: []byte(""),
		}}}
		value, err := pb.Marshal(wrapInLogEntryValue(wr))
		assert.NoError(t, err)

		assert.NoError(t, wal.Append(&proto.LogEntry{
			Term:   5,
			Offset: i,
			Value:  value,
		}))

		_, err = db.ProcessWrite(wr, i-1, 0, kv.NoOpCallback)
		assert.NoError(t, err)
	}

	assert.NoError(t, db.UpdateTerm(5))
	assert.NoError(t, db.Close())
	assert.NoError(t, wal.Close())

	rpcClient := newMockRpcClient()

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, rpcClient, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:    6,
		ShardId: shard,
	})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              6,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": {Term: 5, Offset: 9},
		},
	})
	assert.NoError(t, err)

	/// Add some entries in term 6 that will be only in the
	/// leader and f1
	go func() {
		for i := 0; i < 10; i++ {
			req := <-rpcClient.appendReqs

			rpcClient.ackResps <- &proto.Ack{
				Offset: req.Entry.Offset,
			}
		}
	}()

	/// Write entries
	for i := 10; i < 20; i++ {
		res, err := lc.Write(context.Background(), &proto.WriteRequest{
			ShardId: &shard,
			Puts: []*proto.PutRequest{{
				Key:   "my-key",
				Value: []byte("")}},
		})

		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(res.Puts))
		assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
		assert.EqualValues(t, i, res.Puts[0].Version.VersionId)
	}

	rpcClient.truncateResps <- struct {
		*proto.TruncateResponse
		error
	}{&proto.TruncateResponse{HeadEntryId: &proto.EntryId{Term: 5, Offset: 9}}, nil}

	// Adding a follower that needs to be truncated
	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:      shard,
		Term:         6,
		FollowerName: "f2",
		FollowerHeadEntryId: &proto.EntryId{
			Term:   5,
			Offset: 12,
		},
	})
	assert.NoError(t, err)

	trReq := <-rpcClient.truncateReqs
	assert.Equal(t, common.DefaultNamespace, trReq.Namespace)
	assert.EqualValues(t, 6, trReq.Term)
	AssertProtoEqual(t, &proto.EntryId{Term: 5, Offset: 9}, trReq.HeadEntryId)
	assert.Equal(t, shard, trReq.ShardId)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollowerCheckTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:    5,
		ShardId: shard,
	})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:             shard,
		Term:                4,
		FollowerName:        "f2",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))

	afRes, err = lc.AddFollower(&proto.AddFollowerRequest{
		ShardId:             shard,
		Term:                6,
		FollowerName:        "f2",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// When a leader starts, before we can start to serve write/read requests, we need to ensure
// that all the entries that are in the leader wal are fully committed and applied into the db.
// Otherwise, we could have the scenario where entries were already acked to a client though
// are not appearing when doing a subsequent read if the leader has changed.
func TestLeaderController_EntryVisibilityAfterBecomingLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	wal, err := walFactory.NewWal(common.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	v, err := pb.Marshal(wrapInLogEntryValue(&proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "my-key",
			Value: []byte("my-value"),
		}},
	}))
	assert.NoError(t, err)
	assert.NoError(t, wal.Append(&proto.LogEntry{
		Term:   0,
		Offset: 0,
		Value:  v,
	}))

	rpc := newMockRpcClient()

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, rpc, walFactory, kvFactory)

	_, _ = lc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    1,
	})

	// Respond to replication flow to follower
	go func() {
		req := <-rpc.appendReqs

		rpc.ackResps <- &proto.Ack{
			Offset: req.Entry.Offset,
		}
	}()

	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{
			// The follower does not have the entry in its wal yet
			"f1": {Term: 0, Offset: -1},
		},
	})

	/// We should be able to read the entry, even if it was not fully committed before the leader started
	r := <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "my-key", IncludeValue: true}},
	})

	assert.NoError(t, r.Err)
	assert.Equal(t, proto.Status_OK, r.Response.Status)
	assert.Equal(t, []byte("my-value"), r.Response.Value)
	assert.EqualValues(t, 0, r.Response.Version.VersionId)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_Notifications(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
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
	_, _ = lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	nb1 := <-stream.ch
	assert.EqualValues(t, 0, nb1.Offset)
	assert.Equal(t, 1, len(nb1.Notifications))
	n1 := nb1.Notifications["a"]
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n1.Type)
	assert.EqualValues(t, 0, *n1.VersionId)

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
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
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

func TestLeaderController_List(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte{0}},
			{Key: "/b", Value: []byte{0}},
			{Key: "/c", Value: []byte{0}},
			{Key: "/d", Value: []byte{0}},
		},
	})
	assert.NoError(t, err)

	list, err := lc.ListSliceNoMutex(context.Background(), &proto.ListRequest{
		ShardId:        &shard,
		StartInclusive: "/a",
		EndExclusive:   "/c",
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, list)

	list, err = lc.ListSliceNoMutex(context.Background(), &proto.ListRequest{
		ShardId:        &shard,
		StartInclusive: "/y",
		EndExclusive:   "/z",
	})
	assert.NoError(t, err)
	assert.Len(t, list, 0)
}

func TestLeaderController_DeleteShard(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts:    []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: common.DefaultNamespace,
		ShardId:   shard,
		Term:      1,
	})
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())

	lc, _ = NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	/// Read entry
	r := <-lc.Read(context.Background(), &proto.ReadRequest{
		ShardId: &shard,
		Gets:    []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	})

	assert.NoError(t, r.Err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r.Response.Status)
	assert.Nil(t, r.Response.Value)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_DeleteShard_WrongTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts:    []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: common.DefaultNamespace,
		ShardId:   shard,
		Term:      0,
	})
	assert.ErrorIs(t, err, common.ErrorInvalidTerm)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_GetStatus(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	/// Write entry
	_, _ = lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})
	_, _ = lc.Write(context.Background(), &proto.WriteRequest{
		ShardId: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "b",
			Value: []byte("value-b")}},
	})

	res, err := lc.GetStatus(&proto.GetStatusRequest{ShardId: shard})
	assert.NoError(t, err)
	assert.Equal(t, &proto.GetStatusResponse{
		Term:         2,
		Status:       proto.ServingStatus_LEADER,
		HeadOffset:   1,
		CommitOffset: 1,
	}, res)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
