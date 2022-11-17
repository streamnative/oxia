package batch

import (
	"github.com/stretchr/testify/assert"
	"io"
	"oxia/proto"
	"reflect"
	"sync"
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
		assert.Equal(t, item.expectedSize, batch.Size(), callType)
	}
}

func TestWriteBatchComplete(t *testing.T) {
	putResponseOk := &proto.PutResponse{
		Status: proto.Status_OK,
		Stat: &proto.Stat{
			Version:           1,
			CreatedTimestamp:  2,
			ModifiedTimestamp: 3,
		},
	}
	for _, item := range []struct {
		response                    *proto.WriteResponse
		err                         error
		expectedPutResponse         *proto.PutResponse
		expectedPutErr              error
		expectedDeleteResponse      *proto.DeleteResponse
		expectedDeleteErr           error
		expectedDeleteRangeResponse *proto.DeleteRangeResponse
		expectedDeleteRangeErr      error
	}{
		{
			&proto.WriteResponse{
				Puts: []*proto.PutResponse{putResponseOk},
				Deletes: []*proto.DeleteResponse{{
					Status: proto.Status_OK,
				}},
				DeleteRanges: []*proto.DeleteRangeResponse{{
					Status: proto.Status_OK,
				}},
			},
			nil,
			putResponseOk,
			nil,
			&proto.DeleteResponse{
				Status: proto.Status_OK,
			},
			nil,
			&proto.DeleteRangeResponse{
				Status: proto.Status_OK,
			},
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
			&proto.PutResponse{
				Status: proto.Status_BAD_VERSION,
			},
			nil,
			&proto.DeleteResponse{
				Status: proto.Status_KEY_NOT_FOUND,
			},
			nil,
			&proto.DeleteRangeResponse{
				Status: proto.Status_OK,
			},
			nil,
		},
		{
			nil,
			io.EOF,
			nil,
			io.EOF,
			nil,
			io.EOF,
			nil,
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
					StartInclusive: "/callC",
					EndExclusive:   "/d",
				}},
			}, request)
			return item.response, item.err
		}

		factory := &writeBatchFactory{execute: execute}
		batch := factory.newBatch(&shardId)

		var wg sync.WaitGroup
		wg.Add(3)

		var putResponse *proto.PutResponse
		var putErr error
		var deleteResponse *proto.DeleteResponse
		var deleteErr error
		var deleteRangeResponse *proto.DeleteRangeResponse
		var deleteRangeErr error

		putCallback := func(response *proto.PutResponse, err error) {
			putResponse = response
			putErr = err
			wg.Done()
		}
		deleteCallback := func(response *proto.DeleteResponse, err error) {
			deleteResponse = response
			deleteErr = err
			wg.Done()
		}
		deleteRangeCallback := func(response *proto.DeleteRangeResponse, err error) {
			deleteRangeResponse = response
			deleteRangeErr = err
			wg.Done()
		}

		batch.Add(PutCall{
			Key:             "/a",
			Payload:         []byte{0},
			ExpectedVersion: &one,
			Callback:        putCallback,
		})
		batch.Add(DeleteCall{
			Key:             "/b",
			ExpectedVersion: &two,
			Callback:        deleteCallback,
		})
		batch.Add(DeleteRangeCall{
			MinKeyInclusive: "/callC",
			MaxKeyExclusive: "/d",
			Callback:        deleteRangeCallback,
		})
		assert.Equal(t, 3, batch.Size())

		batch.Complete()

		wg.Wait()

		assert.Equal(t, item.expectedPutResponse, putResponse)
		assert.ErrorIs(t, item.expectedPutErr, putErr)

		assert.Equal(t, item.expectedDeleteResponse, deleteResponse)
		assert.ErrorIs(t, item.expectedDeleteErr, deleteErr)

		assert.Equal(t, item.expectedDeleteRangeResponse, deleteRangeResponse)
		assert.ErrorIs(t, item.expectedDeleteRangeErr, deleteRangeErr)
	}
}
