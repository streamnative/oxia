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
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"
	"oxia/common"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
	"time"
)

func TestFollowerCursor(t *testing.T) {
	var term int64 = 1
	var shard uint32 = 2

	stream := newMockRpcClient()
	ackTracker := NewQuorumAckTracker(3, wal.InvalidOffset, wal.InvalidOffset)
	kvf, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := kv.NewDB(shard, kvf, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})
	w, err := wf.NewWal(shard)
	assert.NoError(t, err)

	err = w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 0,
		Value:  []byte("v1"),
	})
	assert.NoError(t, err)
	log.Logger.Info().Msg("Appended entry 0 to the log")

	fc, err := NewFollowerCursor("f1", term, shard, stream, ackTracker, w, db, wal.InvalidOffset)
	assert.NoError(t, err)

	assert.Equal(t, shard, fc.ShardId())
	assert.Equal(t, wal.InvalidOffset, fc.LastPushed())
	assert.Equal(t, wal.InvalidOffset, fc.AckOffset())

	assert.Eventually(t, func() bool {
		return fc.LastPushed() == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.InvalidOffset, fc.AckOffset())

	ackTracker.AdvanceHeadOffset(0)

	assert.Equal(t, wal.InvalidOffset, fc.AckOffset())

	// The follower is acking back
	req := <-stream.appendReqs
	assert.EqualValues(t, 1, req.Term)
	assert.Equal(t, wal.InvalidOffset, req.CommitOffset)

	stream.ackResps <- &proto.Ack{
		Offset: 0,
	}

	assert.Eventually(t, func() bool {
		return fc.AckOffset() == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 0, ackTracker.CommitOffset())

	// Next entry should carry the correct commit offset
	err = w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 1,
		Value:  []byte("v2"),
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 0, fc.LastPushed())
	assert.EqualValues(t, 0, fc.AckOffset())

	ackTracker.AdvanceHeadOffset(1)

	assert.Eventually(t, func() bool {
		return fc.LastPushed() == 1
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 0, fc.AckOffset())

	req = <-stream.appendReqs
	assert.EqualValues(t, 1, req.Term)
	assert.EqualValues(t, 1, req.Entry.Term)
	assert.EqualValues(t, 1, req.Entry.Offset)
	assert.EqualValues(t, 0, req.CommitOffset)

	assert.NoError(t, fc.Close())
}

func TestFollowerCursor_SendSnapshot(t *testing.T) {
	var term int64 = 1
	var shard uint32 = 2

	N := int64(10)
	stream := newMockRpcClient()
	kvf, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	db, err := kv.NewDB(shard, kvf, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})
	w, err := wf.NewWal(shard)
	assert.NoError(t, err)

	// Load some entries into the db & wal
	for i := int64(0); i < N; i++ {
		wr := &proto.WriteRequest{
			ShardId: &shard,
			Puts: []*proto.PutRequest{{
				Key:     fmt.Sprintf("key-%d", i),
				Payload: []byte(fmt.Sprintf("value-%d", i)),
			}},
		}
		e, _ := pb.Marshal(wrapInLogEntryValue(wr))
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:      1,
			Offset:    i,
			Value:     e,
			Timestamp: uint64(i),
		}))

		_, err := db.ProcessWrite(wr, i, uint64(i), kv.NoOpCallback)
		assert.NoError(t, err)
	}

	ackTracker := NewQuorumAckTracker(3, N-1, N-1)

	fc, err := NewFollowerCursor("f1", term, shard, stream, ackTracker, w, db, wal.InvalidOffset)
	assert.NoError(t, err)

	s := stream.sendSnapshotStream
	for req := range s.requests {
		assert.EqualValues(t, 1, req.Term)
	}

	log.Info().Msg("Snapshot complete")

	s.response <- &proto.SnapshotResponse{AckOffset: N - 1}

	assert.Eventually(t, func() bool {
		return fc.AckOffset() == N-1
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
}

func wrapInLogEntryValue(wr *proto.WriteRequest) *proto.LogEntryValue {
	return &proto.LogEntryValue{
		Value: &proto.LogEntryValue_Requests{
			Requests: &proto.WriteRequests{
				Writes: []*proto.WriteRequest{
					wr,
				},
			},
		},
	}
}
