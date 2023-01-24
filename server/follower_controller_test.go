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
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"
	"oxia/common"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
	"testing"
	"time"
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

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, truncateResp.HeadEntryId.Term)
	assert.Equal(t, wal.InvalidOffset, truncateResp.HeadEntryId.Offset)

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	stream := newMockServerReplicateStream()

	wg := common.NewWaitGroup(1)

	go func() {
		err := fc.Replicate(stream)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for response
	response := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.EqualValues(t, 0, response.Offset)

	// Write next entry
	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))

	// Wait for response
	response = stream.GetResponse()
	assert.EqualValues(t, 1, response.Offset)

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// Double-check the values in the DB
	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:          "a",
		IncludeValue: true}, {
		Key:          "b",
		IncludeValue: true,
	},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	// Keys are not there because they were not part of the commit offset
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Gets[0].Status)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Gets[1].Status)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())

	_ = wg.Wait(context.Background())
}

func TestReadingUpToCommitOffset(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	_, err = fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   0,
			Offset: wal.InvalidOffset,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	stream := newMockServerReplicateStream()
	go func() {
		//cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "2", "b": "3"},
		// Commit offset points to previous entry
		0))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.EqualValues(t, 0, r1.Offset)

	r2 := stream.GetResponse()

	assert.EqualValues(t, 1, r2.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:          "a",
		IncludeValue: true}, {
		Key:          "b",
		IncludeValue: true,
	},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	// Keys are not there because they were not part of the commit offset
	assert.Equal(t, proto.Status_OK, dbRes.Gets[0].Status)
	assert.Equal(t, []byte("0"), dbRes.Gets[0].Value)
	assert.Equal(t, proto.Status_OK, dbRes.Gets[1].Status)
	assert.Equal(t, []byte("1"), dbRes.Gets[1].Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_RestoreCommitOffset(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	db, err := kv.NewDB(shardId, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:   "xx",
		Value: []byte(""),
	}}}, 9, 0, kv.NoOpCallback)
	assert.NoError(t, err)

	assert.NoError(t, db.UpdateTerm(6))
	assert.NoError(t, db.Close())

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 6, fc.Term())
	assert.EqualValues(t, 9, fc.CommitOffset())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// If a follower receives a commit offset from the leader that is ahead
// of the current follower head offset, it needs to advance the commit
// offset only up to the current head.
func TestFollower_AdvanceCommitOffsetToHead(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, _ := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()
	go func() {
		//cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 10))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.EqualValues(t, 0, r1.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_NewTerm(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// We cannot fence with earlier term
	fr, err := fc.NewTerm(&proto.NewTermRequest{Term: 0})
	assert.Nil(t, fr)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// A fence with same term needs to be accepted
	fr, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NotNil(t, fr)
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// Higher term will work
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 3})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 3, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// If a node is restarted, it might get the truncate request
// when it's in the `NotMember` state. That is ok, provided
// the request comes in the same term that the follower
// currently has
func TestFollower_TruncateAfterRestart(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	// Follower needs to be in "Fenced" state to receive a Truncate request
	tr, err := fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   0,
			Offset: 0,
		},
	})

	assert.Equal(t, common.CodeInvalidStatus, status.Code(err))
	assert.Nil(t, tr)
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	_, err = fc.NewTerm(&proto.NewTermRequest{
		ShardId: shardId,
		Term:    2,
	})
	assert.NoError(t, err)
	fc.Close()

	// Restart
	fc, err = NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())

	tr, err = fc.Truncate(&proto.TruncateRequest{
		Term: 2,
		HeadEntryId: &proto.EntryId{
			Term:   -1,
			Offset: -1,
		},
	})

	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: 2, Offset: -1}, tr.HeadEntryId)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_PersistentTerm(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: t.TempDir(),
	})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())
	assert.Equal(t, wal.InvalidTerm, fc.Term())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 4})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 4, fc.Term())

	assert.NoError(t, fc.Close())

	/// Reopen and verify term
	fc, err = NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 4, fc.Term())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_CommitOffsetLastEntry(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := newMockServerReplicateStream()
	go func() {
		//cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.EqualValues(t, 0, r1.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:          "a",
		IncludeValue: true}, {
		Key:          "b",
		IncludeValue: true,
	},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	assert.Equal(t, proto.Status_OK, dbRes.Gets[0].Status)
	assert.Equal(t, []byte("0"), dbRes.Gets[0].Value)
	assert.Equal(t, proto.Status_OK, dbRes.Gets[1].Status)
	assert.Equal(t, []byte("1"), dbRes.Gets[1].Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollowerController_RejectEntriesWithDifferentTerm(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
	})
	assert.NoError(t, err)

	db, err := kv.NewDB(shardId, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5))
	assert.NoError(t, db.Close())

	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	stream := newMockServerReplicateStream()
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "1", "b": "1"}, wal.InvalidOffset))

	// Follower will reject the entry because it's from an earlier term
	err = fc.Replicate(stream)
	assert.Error(t, err)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	// If we send an entry of same term, it will be accepted
	stream.AddRequest(createAddRequest(t, 5, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))

	go func() {
		//cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	assert.NoError(t, fc.Close())
	close(stream.requests)

	//// A higher term will also be rejected
	fc, err = NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	stream = newMockServerReplicateStream()
	stream.AddRequest(createAddRequest(t, 6, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))
	err = fc.Replicate(stream)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err), "Unexpected error: %s", err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_RejectTruncateInvalidTerm(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 5})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	// Lower term should be rejected
	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Term: 4,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.Nil(t, truncateResp)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	// Truncate with higher term should also fail
	truncateResp, err = fc.Truncate(&proto.TruncateRequest{
		Term: 6,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.Nil(t, truncateResp)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())
}

func prepareTestDb(t *testing.T) kv.Snapshot {
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir: t.TempDir(),
	})
	assert.NoError(t, err)
	db, err := kv.NewDB(0, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{
				Key:   fmt.Sprintf("key-%d", i),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			}},
		}, int64(i), 0, kv.NoOpCallback)
		assert.NoError(t, err)
	}

	snapshot, err := db.Snapshot()
	assert.NoError(t, err)

	assert.NoError(t, kvFactory.Close())

	return snapshot
}

func TestFollower_HandleSnapshot(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir: t.TempDir(),
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := newMockServerReplicateStream()
	go func() { assert.NoError(t, fc.Replicate(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	close(stream.requests)

	// Load snapshot into follower
	snapshot := prepareTestDb(t)

	snapshotStream := newMockServerSendSnapshotStream()
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := fc.SendSnapshot(snapshotStream)
		assert.NoError(t, err)
		wg.Done()
	}()

	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		assert.NoError(t, err)
		content := chunk.Content()
		snapshotStream.AddChunk(&proto.SnapshotChunk{
			Term:       1,
			Name:       chunk.Name(),
			Content:    content,
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}

	close(snapshotStream.chunks)

	// Wait for follower to fully load the snapshot
	wg.Wait()

	// At this point the content of the follower should only include the
	// data from the snapshot and any existing data should be gone

	dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
		Key:          "a",
		IncludeValue: true}, {
		Key:          "b",
		IncludeValue: true,
	}}})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbRes.Gets))
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Gets[0].Status)
	assert.Nil(t, dbRes.Gets[0].Value)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Gets[1].Status)
	assert.Nil(t, dbRes.Gets[1].Value)

	for i := 0; i < 100; i++ {
		dbRes, err := fc.(*followerController).db.ProcessRead(&proto.ReadRequest{Gets: []*proto.GetRequest{{
			Key:          fmt.Sprintf("key-%d", i),
			IncludeValue: true,
		},
		}})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(dbRes.Gets))
		assert.Equal(t, proto.Status_OK, dbRes.Gets[0].Status)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), dbRes.Gets[0].Value)
	}

	assert.Equal(t, wal.InvalidOffset, fc.(*followerController).wal.LastOffset())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_DisconnectLeader(t *testing.T) {
	var shardId uint32
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	fc, _ := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()

	go func() { assert.NoError(t, fc.Replicate(stream)) }()

	assert.Eventually(t, closeChanIsNotNil(fc), 10*time.Second, 10*time.Millisecond)

	// It's not possible to add a new leader stream
	assert.ErrorIs(t, fc.Replicate(stream), common.ErrorLeaderAlreadyConnected)

	// When we fence again, the leader should have been cutoff
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 2})
	assert.NoError(t, err)

	assert.Nil(t, fc.(*followerController).closeStreamWg)

	go func() {
		//cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	assert.Eventually(t, closeChanIsNotNil(fc), 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_DupEntries(t *testing.T) {
	var shardId uint32
	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	fc, _ := NewFollowerController(Config{}, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()
	go func() {
		//cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for responses
	r1 := stream.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	r2 := stream.GetResponse()
	assert.EqualValues(t, 0, r2.Offset)

	// Write next entry
	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))
	r3 := stream.GetResponse()
	assert.EqualValues(t, 1, r3.Offset)

	// Go back with older offset
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))
	r4 := stream.GetResponse()
	assert.EqualValues(t, 0, r4.Offset)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollowerController_Closed(t *testing.T) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()

	fc, err := NewFollowerController(Config{}, shard, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, fc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	assert.NoError(t, fc.Close())

	res, err := fc.NewTerm(&proto.NewTermRequest{
		ShardId: shard,
		Term:    2,
	})

	assert.Nil(t, res)
	assert.Equal(t, common.CodeAlreadyClosed, status.Code(err))

	res2, err := fc.Truncate(&proto.TruncateRequest{
		ShardId: shard,
		Term:    2,
		HeadEntryId: &proto.EntryId{
			Term:   2,
			Offset: 1,
		},
	})

	assert.Nil(t, res2)
	assert.Equal(t, common.CodeAlreadyClosed, status.Code(err))

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func closeChanIsNotNil(fc FollowerController) func() bool {
	return func() bool {
		_fc := fc.(*followerController)
		_fc.Lock()
		defer _fc.Unlock()
		return _fc.closeStreamWg != nil
	}
}

func createAddRequest(t *testing.T, term int64, offset int64,
	kvs map[string]string,
	commitOffset int64) *proto.Append {
	br := &proto.WriteRequest{}

	for k, v := range kvs {
		br.Puts = append(br.Puts, &proto.PutRequest{
			Key:   k,
			Value: []byte(v),
		})
	}

	entry, err := pb.Marshal(br)
	assert.NoError(t, err)

	le := &proto.LogEntry{
		Term:   term,
		Offset: offset,
		Value:  entry,
	}

	return &proto.Append{
		Term:         term,
		Entry:        le,
		CommitOffset: commitOffset,
	}
}
