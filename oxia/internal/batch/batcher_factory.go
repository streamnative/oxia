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
	"oxia/oxia/internal"
	"oxia/oxia/internal/metrics"
	"time"
)

type BatcherFactory struct {
	Executor            internal.Executor
	Linger              time.Duration
	MaxRequestsPerBatch int
	BatcherBufferSize   int
	RequestTimeout      time.Duration
	Metrics             *metrics.Metrics
}

func (b *BatcherFactory) NewWriteBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, writeBatchFactory{
		execute:        b.Executor.ExecuteWrite,
		metrics:        b.Metrics,
		requestTimeout: b.RequestTimeout,
	}.newBatch)
}

func (b *BatcherFactory) NewReadBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, readBatchFactory{
		execute:        b.Executor.ExecuteRead,
		metrics:        b.Metrics,
		requestTimeout: b.RequestTimeout,
	}.newBatch)
}

func (b *BatcherFactory) newBatcher(shardId *uint32, batchFactory func(shardId *uint32) Batch) Batcher {
	batcher := &batcherImpl{
		shardId:             shardId,
		batchFactory:        batchFactory,
		callC:               make(chan any, b.BatcherBufferSize),
		closeC:              make(chan bool),
		linger:              b.Linger,
		maxRequestsPerBatch: b.MaxRequestsPerBatch,
	}

	go batcher.run()

	return batcher
}
