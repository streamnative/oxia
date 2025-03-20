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
					SequenceKeyDelta: []uint64{1},
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
