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
