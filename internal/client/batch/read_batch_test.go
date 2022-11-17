package batch

import (
	"github.com/stretchr/testify/assert"
	"io"
	"oxia/oxia"
	"oxia/proto"
	"reflect"
	"testing"
)

func TestReadBatchAdd(t *testing.T) {
	for _, item := range []struct {
		call         any
		expectPanic  bool
		expectedSize int
	}{
		{PutCall{}, true, 0},
		{DeleteCall{}, true, 0},
		{DeleteRangeCall{}, true, 0},
		{GetCall{}, false, 1},
		{GetRangeCall{}, false, 1},
	} {
		factory := &readBatchFactory{}
		batch := factory.newBatch(&shardId)

		panicked := add(batch, item.call)

		callType := reflect.TypeOf(item.call)
		assert.Equal(t, item.expectPanic, panicked, callType)
		assert.Equal(t, item.expectedSize, batch.size(), callType)
	}
}

func TestReadBatchComplete(t *testing.T) {
	for _, item := range []struct {
		response               *proto.ReadResponse
		err                    error
		expectedGetResult      oxia.GetResult
		expectedGetRangeResult oxia.GetRangeResult
	}{
		{
			&proto.ReadResponse{
				Gets: []*proto.GetResponse{{
					Status: proto.Status_OK,
					Stat: &proto.Stat{
						Version:           1,
						CreatedTimestamp:  2,
						ModifiedTimestamp: 3,
					},
					Payload: []byte{0},
				}},
				GetRanges: []*proto.GetRangeResponse{{
					Keys: []string{"/a"},
				}},
			},
			nil,
			oxia.GetResult{
				Payload: []byte{0},
				Stat: oxia.Stat{
					Version:           1,
					CreatedTimestamp:  2,
					ModifiedTimestamp: 3,
				},
			},
			oxia.GetRangeResult{
				Keys: []string{"/a"},
			},
		},
		{
			&proto.ReadResponse{
				Gets: []*proto.GetResponse{{
					Status: proto.Status_KEY_NOT_FOUND,
				}},
				GetRanges: []*proto.GetRangeResponse{{
					Keys: []string{"/a"},
				}},
			},
			nil,
			oxia.GetResult{
				Err: oxia.ErrorKeyNotFound,
			},
			oxia.GetRangeResult{
				Keys: []string{"/a"},
			},
		},
		{
			nil,
			io.EOF,
			oxia.GetResult{
				Err: io.EOF,
			},
			oxia.GetRangeResult{
				Err: io.EOF,
			},
		},
	} {
		execute := func(request *proto.ReadRequest) (*proto.ReadResponse, error) {
			assert.Equal(t, &proto.ReadRequest{
				ShardId: &shardId,
				Gets: []*proto.GetRequest{{
					Key:            "/a",
					IncludePayload: true,
				}},
				GetRanges: []*proto.GetRangeRequest{{
					StartInclusive: "/b",
					EndExclusive:   "/c",
				}},
			}, request)
			return item.response, item.err
		}

		factory := &readBatchFactory{execute: execute}
		batch := factory.newBatch(&shardId)

		getC := make(chan oxia.GetResult, 1)
		getRangeC := make(chan oxia.GetRangeResult, 1)

		batch.add(GetCall{
			Key: "/a",
			C:   getC,
		})
		batch.add(GetRangeCall{
			MinKeyInclusive: "/b",
			MaxKeyExclusive: "/c",
			C:               getRangeC,
		})
		assert.Equal(t, 2, batch.size())

		batch.complete()

		getResult, ok := <-getC
		assert.Equal(t, item.expectedGetResult, getResult)
		assert.True(t, ok)
		getResult, ok = <-getC
		assert.Equal(t, oxia.GetResult{}, getResult)
		assert.False(t, ok)

		err, ok := <-getRangeC
		assert.Equal(t, item.expectedGetRangeResult, err)
		assert.True(t, ok)
		err, ok = <-getRangeC
		assert.Equal(t, oxia.GetRangeResult{}, err)
		assert.False(t, ok)
	}
}
