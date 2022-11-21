package batch

import (
	"github.com/stretchr/testify/assert"
	"io"
	"oxia/proto"
	"reflect"
	"sync"
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
		assert.Equal(t, item.expectedSize, batch.Size(), callType)
	}
}

func TestReadBatchComplete(t *testing.T) {
	getResponseOk := &proto.GetResponse{
		Payload: []byte{0},
		Status:  proto.Status_OK,
		Version: &proto.Version{
			VersionId:         1,
			CreatedTimestamp:  2,
			ModifiedTimestamp: 3,
		},
	}
	for _, item := range []struct {
		response                 *proto.ReadResponse
		err                      error
		expectedGetResponse      *proto.GetResponse
		expectedGetErr           error
		expectedGetRangeResponse *proto.GetRangeResponse
		expectedGetRangeErr      error
	}{
		{
			&proto.ReadResponse{
				Gets: []*proto.GetResponse{getResponseOk},
				GetRanges: []*proto.GetRangeResponse{{
					Keys: []string{"/a"},
				}},
			},
			nil,
			getResponseOk,
			nil,
			&proto.GetRangeResponse{
				Keys: []string{"/a"},
			},
			nil,
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
			&proto.GetResponse{
				Status: proto.Status_KEY_NOT_FOUND,
			},
			nil,
			&proto.GetRangeResponse{
				Keys: []string{"/a"},
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
					EndExclusive:   "/callC",
				}},
			}, request)
			return item.response, item.err
		}

		factory := &readBatchFactory{execute: execute}
		batch := factory.newBatch(&shardId)

		var wg sync.WaitGroup
		wg.Add(3)

		var getResponse *proto.GetResponse
		var getErr error
		var getRangeResponse *proto.GetRangeResponse
		var getRangeErr error

		getCallback := func(response *proto.GetResponse, err error) {
			getResponse = response
			getErr = err
			wg.Done()
		}
		getRangeCallback := func(response *proto.GetRangeResponse, err error) {
			getRangeResponse = response
			getRangeErr = err
			wg.Done()
		}

		batch.Add(GetCall{
			Key:      "/a",
			Callback: getCallback,
		})
		batch.Add(GetRangeCall{
			MinKeyInclusive: "/b",
			MaxKeyExclusive: "/callC",
			Callback:        getRangeCallback,
		})
		assert.Equal(t, 2, batch.Size())

		batch.Complete()

		assert.Equal(t, item.expectedGetResponse, getResponse)
		assert.ErrorIs(t, item.expectedGetErr, getErr)

		assert.Equal(t, item.expectedGetRangeResponse, getRangeResponse)
		assert.ErrorIs(t, item.expectedGetRangeErr, getRangeErr)
	}
}
