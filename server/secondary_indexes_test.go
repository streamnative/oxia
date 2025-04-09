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
	"testing"

	"github.com/streamnative/oxia/common/callback"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
)

func TestSecondaryIndices_List(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "1"}}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "2"}}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "3"}}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "4"}}},
		},
	})
	assert.NoError(t, err)

	strCh, err := lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	k := <-strCh
	assert.Equal(t, "/b", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	// NOTE: we cannot use assert.Empty to check strCh and must to validate it's closed, because when there is no more
	// items to return, assert.Empty will passthrough and leave the iterator opened.
	_, ok := <-strCh
	assert.False(t, ok)

	// Wrong index
	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	})
	assert.NoError(t, err)
	_, ok = <-strCh
	assert.False(t, ok)

	// Individual delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	k = <-strCh
	assert.Equal(t, "/a", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)
	_, ok = <-strCh
	assert.False(t, ok)

	// Range delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)
	_, ok = <-strCh
	assert.False(t, ok)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestSecondaryIndices_RangeScan(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "1"}}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "2"}}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "3"}}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "4"}}},
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan *proto.GetResponse, 100)
	errCh := make(chan error, 1)
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	}, callback.ReadFromStreamCallback(ch, errCh))
	assert.NoError(t, err)

	gr := <-ch
	assert.Equal(t, "/b", *gr.Key)
	assert.Equal(t, "1", string(gr.Value))
	gr = <-ch
	assert.Equal(t, "/c", *gr.Key)
	assert.Equal(t, "2", string(gr.Value))
	assert.Empty(t, ch)

	assert.Empty(t, errCh)

	ch = make(chan *proto.GetResponse, 100)
	errCh = make(chan error, 1)
	// Wrong index
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	}, callback.ReadFromStreamCallback(ch, errCh))
	assert.Empty(t, ch)
	assert.Empty(t, errCh)

	// Individual delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	ch = make(chan *proto.GetResponse, 100)
	errCh = make(chan error, 1)
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	}, callback.ReadFromStreamCallback(ch, errCh))

	gr = <-ch
	assert.Equal(t, "/a", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/c", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/d", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/e", *gr.Key)
	assert.Empty(t, ch)
	assert.Empty(t, errCh)

	assert.Empty(t, errCh)

	// Range delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	ch = make(chan *proto.GetResponse, 100)
	errCh = make(chan error, 1)
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	}, callback.ReadFromStreamCallback(ch, errCh))

	gr = <-ch
	assert.Equal(t, "/d", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/e", *gr.Key)
	assert.Empty(t, ch)
	assert.Empty(t, errCh)

	cancel()
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestSecondaryIndices_MultipleKeysForSameIdx(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "a"},
				{IndexName: "idx", SecondaryKey: "A"},
			}},
			{Key: "/b", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "b"},
				{IndexName: "idx", SecondaryKey: "B"},
			}},
			{Key: "/c", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "c"},
				{IndexName: "idx", SecondaryKey: "C"},
			}},
			{Key: "/d", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "d"},
				{IndexName: "idx", SecondaryKey: "D"},
			}},
			{Key: "/e", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "e"},
				{IndexName: "idx", SecondaryKey: "E"},
			}},
		},
	})
	assert.NoError(t, err)

	strCh, err := lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "b",
		EndExclusive:       "d",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)

	k := <-strCh
	assert.Equal(t, "/b", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	assert.Empty(t, strCh)

	// using alternate values on same index
	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "B",
		EndExclusive:       "D",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)

	k = <-strCh
	assert.Equal(t, "/b", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	assert.Empty(t, strCh)

	// Repeated primary keys when multiple indexes
	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "A",
		EndExclusive:       "z",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)

	k = <-strCh
	assert.Equal(t, "/a", k)
	k = <-strCh
	assert.Equal(t, "/b", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)
	k = <-strCh
	assert.Equal(t, "/a", k)
	k = <-strCh
	assert.Equal(t, "/b", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)
	assert.Empty(t, strCh)

	// Delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "a",
		EndExclusive:       "z",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)
	k = <-strCh
	assert.Equal(t, "/a", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)

	assert.Empty(t, strCh)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
