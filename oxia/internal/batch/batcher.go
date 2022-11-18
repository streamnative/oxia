package batch

import (
	"errors"
	"io"
	"oxia/oxia/internal"
	"time"
)

var ErrorShuttingDown = errors.New("shutting down")

type BatcherFactory struct {
	Executor internal.Executor
	Linger   time.Duration
	MaxSize  int
}

func (b *BatcherFactory) NewWriteBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, writeBatchFactory{execute: b.Executor.ExecuteWrite}.newBatch)
}

func (b *BatcherFactory) NewReadBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, readBatchFactory{execute: b.Executor.ExecuteRead}.newBatch)
}

func (b *BatcherFactory) newBatcher(shardId *uint32, batchFactory func(shardId *uint32) Batch) Batcher {
	batcher := &batcherImpl{
		shardId:      shardId,
		batchFactory: batchFactory,
		callC:        make(chan any),
		closeC:       make(chan bool),
		linger:       b.Linger,
		maxSize:      b.MaxSize,
	}
	go batcher.run()
	return batcher
}

//////////

type Batcher interface {
	io.Closer
	Add(request any)
	run()
}

type batcherImpl struct {
	shardId      *uint32
	batchFactory func(shardId *uint32) Batch
	callC        chan any
	closeC       chan bool
	linger       time.Duration
	maxSize      int
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
				timer = time.NewTimer(b.linger)
				timeout = timer.C
			}
			batch.Add(call)
			if batch.Size() == b.maxSize {
				timer.Stop()
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
