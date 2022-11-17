package batch

import (
	"github.com/stretchr/testify/assert"
	"io"
	"oxia/oxia"
	"oxia/proto"
	"reflect"
	"testing"
)

func TestWriteBatchAdd(t *testing.T) {
	for _, item := range []struct {
		call         any
		expectPanic  bool
		expectedSize int
	}{
		{PutCall{}, false, 1},
		{DeleteCall{}, false, 1},
		{DeleteRangeCall{}, false, 1},
		{GetCall{}, true, 0},
		{GetRangeCall{}, true, 0},
	} {
		factory := &writeBatchFactory{}
		batch := factory.newBatch(&shardId)

		panicked := add(batch, item.call)

		callType := reflect.TypeOf(item.call)
		assert.Equal(t, item.expectPanic, panicked, callType)
		assert.Equal(t, item.expectedSize, batch.size(), callType)
	}
}

func TestWriteBatchComplete(t *testing.T) {
	for _, item := range []struct {
		response                  *proto.WriteResponse
		err                       error
		expectedPutResult         oxia.PutResult
		expectedDeleteResult      error
		expectedDeleteRangeResult error
	}{
		{
			&proto.WriteResponse{
				Puts: []*proto.PutResponse{{
					Status: proto.Status_OK,
					Stat: &proto.Stat{
						Version:           1,
						CreatedTimestamp:  2,
						ModifiedTimestamp: 3,
					},
				}},
				Deletes: []*proto.DeleteResponse{{
					Status: proto.Status_OK,
				}},
				DeleteRanges: []*proto.DeleteRangeResponse{{
					Status: proto.Status_OK,
				}},
			},
			nil,
			oxia.PutResult{
				Stat: oxia.Stat{
					Version:           1,
					CreatedTimestamp:  2,
					ModifiedTimestamp: 3,
				},
			},
			nil,
			nil,
		},
		{
			&proto.WriteResponse{
				Puts: []*proto.PutResponse{{
					Status: proto.Status_BAD_VERSION,
				}},
				Deletes: []*proto.DeleteResponse{{
					Status: proto.Status_KEY_NOT_FOUND,
				}},
				DeleteRanges: []*proto.DeleteRangeResponse{{
					Status: proto.Status_OK,
				}},
			},
			nil,
			oxia.PutResult{
				Err: oxia.ErrorBadVersion,
			},
			oxia.ErrorKeyNotFound,
			nil,
		},
		{
			nil,
			io.EOF,
			oxia.PutResult{
				Err: io.EOF,
			},
			io.EOF,
			io.EOF,
		},
	} {
		execute := func(request *proto.WriteRequest) (*proto.WriteResponse, error) {
			assert.Equal(t, &proto.WriteRequest{
				ShardId: &shardId,
				Puts: []*proto.PutRequest{{
					Key:             "/a",
					Payload:         []byte{0},
					ExpectedVersion: &one,
				}},
				Deletes: []*proto.DeleteRequest{{
					Key:             "/b",
					ExpectedVersion: &two,
				}},
				DeleteRanges: []*proto.DeleteRangeRequest{{
					StartInclusive: "/c",
					EndExclusive:   "/d",
				}},
			}, request)
			return item.response, item.err
		}

		factory := &writeBatchFactory{execute: execute}
		batch := factory.newBatch(&shardId)

		putC := make(chan oxia.PutResult, 1)
		deleteC := make(chan error, 1)
		deleteRangeC := make(chan error, 1)

		batch.add(PutCall{
			Key:             "/a",
			Payload:         []byte{0},
			ExpectedVersion: &one,
			C:               putC,
		})
		batch.add(DeleteCall{
			Key:             "/b",
			ExpectedVersion: &two,
			C:               deleteC,
		})
		batch.add(DeleteRangeCall{
			MinKeyInclusive: "/c",
			MaxKeyExclusive: "/d",
			C:               deleteRangeC,
		})
		assert.Equal(t, 3, batch.size())

		batch.complete()

		putResult, ok := <-putC
		assert.Equal(t, item.expectedPutResult, putResult)
		assert.True(t, ok)
		putResult, ok = <-putC
		assert.Equal(t, oxia.PutResult{}, putResult)
		assert.False(t, ok)

		err, ok := <-deleteC
		assert.Equal(t, item.expectedDeleteResult, err)
		assert.True(t, ok)
		err, ok = <-deleteC
		assert.Equal(t, nil, err)
		assert.False(t, ok)

		err, ok = <-deleteRangeC
		assert.Equal(t, item.expectedDeleteRangeResult, err)
		assert.True(t, ok)
		err, ok = <-deleteRangeC
		assert.Equal(t, nil, err)
		assert.False(t, ok)
	}
}
