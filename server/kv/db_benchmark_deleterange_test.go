// Copyright 2025 StreamNative, Inc.
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

package kv

import (
	"fmt"
	"testing"
	"time"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"
)

func BenchmarkGenerate100(b *testing.B) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(b, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(b, err)
	defer db.Close()
	benchmarkDeleteRange(db, 100, b)
}

func BenchmarkGenerate1000(b *testing.B) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(b, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(b, err)
	defer db.Close()
	benchmarkDeleteRange(db, 1000, b)
}

func BenchmarkGenerate10000(b *testing.B) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(b, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(b, err)
	defer db.Close()
	benchmarkDeleteRange(db, 10000, b)
}

func benchmarkDeleteRange(db DB, n int, b *testing.B) {
	for i := range b.N * n {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{
				{
					Key:              "00000000000000000001",
					PartitionKey:     pointer.String("00000000000000000001"),
					Value:            []byte("00000000000000000000"),
					SequenceKeyDelta: []uint64{2},
				},
			},
			DeleteRanges: []*proto.DeleteRangeRequest{
				{
					StartInclusive: "00000000000000000001-00000000000000000000",
					EndExclusive:   fmt.Sprintf("00000000000000000001-%020d", i),
				},
			},
		}, int64(i), uint64(time.Now().UnixMilli()), NoOpCallback)
		assert.NoError(b, err)
	}
}
