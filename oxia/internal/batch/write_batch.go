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

package batch

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
	"oxia/common"
	"oxia/common/batch"
	"oxia/oxia/internal/metrics"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"time"
)

type writeBatchFactory struct {
	execute        func(context.Context, *proto.WriteRequest) (*proto.WriteResponse, error)
	metrics        *metrics.Metrics
	requestTimeout time.Duration
}

func (b writeBatchFactory) newBatch(shardId *uint32) batch.Batch {
	return &writeBatch{
		shardId:        shardId,
		execute:        b.execute,
		puts:           make([]model.PutCall, 0),
		deletes:        make([]model.DeleteCall, 0),
		deleteRanges:   make([]model.DeleteRangeCall, 0),
		requestTimeout: b.requestTimeout,
		metrics:        b.metrics,
		callback:       b.metrics.WriteCallback(),
	}
}

//////////

type writeBatch struct {
	shardId        *uint32
	execute        func(context.Context, *proto.WriteRequest) (*proto.WriteResponse, error)
	puts           []model.PutCall
	deletes        []model.DeleteCall
	deleteRanges   []model.DeleteRangeCall
	metrics        *metrics.Metrics
	requestTimeout time.Duration
	callback       func(time.Time, *proto.WriteRequest, *proto.WriteResponse, error)
}

func (b *writeBatch) Add(call any) {
	switch c := call.(type) {
	case model.PutCall:
		b.puts = append(b.puts, b.metrics.DecoratePut(c))
	case model.DeleteCall:
		b.deletes = append(b.deletes, b.metrics.DecorateDelete(c))
	case model.DeleteRangeCall:
		b.deleteRanges = append(b.deleteRanges, b.metrics.DecorateDeleteRange(c))
	default:
		panic("invalid call")
	}
}

func (b *writeBatch) Size() int {
	return len(b.puts) + len(b.deletes) + len(b.deleteRanges)
}

func (b *writeBatch) Complete() {
	executionStart := time.Now()
	request := b.toProto()

	response, err := b.doRequestWithRetries(request)

	b.callback(executionStart, request, response, err)

	if err != nil {
		b.Fail(err)
	} else {
		b.handle(response)
	}
}

func (b *writeBatch) doRequestWithRetries(request *proto.WriteRequest) (response *proto.WriteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()

	backOff := common.NewBackOff(ctx)

	err = backoff.RetryNotify(func() error {
		response, err = b.execute(ctx, request)
		if !isRetriable(err) {
			return backoff.Permanent(err)
		}
		return err
	}, backOff, func(err error, duration time.Duration) {
		log.Logger.Debug().Err(err).
			Dur("retry-after", duration).
			Msg("Failed to perform request, retrying later")
	})

	return response, err
}

func (b *writeBatch) Fail(err error) {
	for _, put := range b.puts {
		put.Callback(nil, err)
	}
	for _, _delete := range b.deletes {
		_delete.Callback(nil, err)
	}
	for _, deleteRange := range b.deleteRanges {
		deleteRange.Callback(nil, err)
	}
}

func (b *writeBatch) handle(response *proto.WriteResponse) {
	for i, put := range b.puts {
		put.Callback(response.Puts[i], nil)
	}
	for i, _delete := range b.deletes {
		_delete.Callback(response.Deletes[i], nil)
	}
	for i, deleteRange := range b.deleteRanges {
		deleteRange.Callback(response.DeleteRanges[i], nil)
	}
}

func (b *writeBatch) toProto() *proto.WriteRequest {
	return &proto.WriteRequest{
		ShardId:      b.shardId,
		Puts:         model.Convert[model.PutCall, *proto.PutRequest](b.puts, model.PutCall.ToProto),
		Deletes:      model.Convert[model.DeleteCall, *proto.DeleteRequest](b.deletes, model.DeleteCall.ToProto),
		DeleteRanges: model.Convert[model.DeleteRangeCall, *proto.DeleteRangeRequest](b.deleteRanges, model.DeleteRangeCall.ToProto),
	}
}
