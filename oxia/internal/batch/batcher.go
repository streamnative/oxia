package batch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"oxia/oxia/internal"
	"oxia/oxia/internal/metrics"
	"runtime/pprof"
	"time"
)

var ErrorShuttingDown = errors.New("shutting down")

type BatcherFactory struct {
	Executor            internal.Executor
	Linger              time.Duration
	MaxRequestsPerBatch int
	Metrics             *metrics.Metrics
}

func (b *BatcherFactory) NewWriteBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, writeBatchFactory{
		execute: b.Executor.ExecuteWrite,
		metrics: b.Metrics,
	}.newBatch)
}

func (b *BatcherFactory) NewReadBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, readBatchFactory{
		execute: b.Executor.ExecuteRead,
		metrics: b.Metrics,
	}.newBatch)
}

func (b *BatcherFactory) newBatcher(shardId *uint32, batchFactory func(shardId *uint32) Batch) Batcher {
	batcher := &batcherImpl{
		shardId:             shardId,
		batchFactory:        batchFactory,
		callC:               make(chan any),
		closeC:              make(chan bool),
		linger:              b.Linger,
		maxRequestsPerBatch: b.MaxRequestsPerBatch,
	}
	go pprof.Do(context.Background(),
		pprof.Labels("oxia", "batcher",
			"shard", fmt.Sprintf("%d", *shardId)),
		func(_ context.Context) {
			batcher.run()
		})

	return batcher
}

//////////

type Batcher interface {
	io.Closer
	Add(request any)
	run()
}

type batcherImpl struct {
	shardId             *uint32
	batchFactory        func(shardId *uint32) Batch
	callC               chan any
	closeC              chan bool
	linger              time.Duration
	maxRequestsPerBatch int
}

func (b *batcherImpl) Close() error {
	close(b.closeC)
	return nil
}

func (b *batcherImpl) Add(call any) {
	b.callC <- call
}

func (b *batcherImpl) run() {
	var batch Batch
	var timer *time.Timer = nil
	var timeout <-chan time.Time = nil
	for {
		select {
		case call := <-b.callC:
			if batch == nil {
				batch = b.batchFactory(b.shardId)
				if b.linger > 0 {
					timer = time.NewTimer(b.linger)
					timeout = timer.C
				}
			}
			batch.Add(call)
			if batch.Size() == b.maxRequestsPerBatch || b.linger == 0 {
				if b.linger > 0 {
					timer.Stop()
				}
				batch.Complete()
				batch = nil
			}
		case <-timeout:
			timer.Stop()
			batch.Complete()
			batch = nil
		case <-b.closeC:
			if batch != nil {
				timer.Stop()
				batch.Fail(ErrorShuttingDown)
				batch = nil
			}
			return
		}
	}
}
