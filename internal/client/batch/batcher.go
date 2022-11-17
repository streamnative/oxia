package batch

import (
	"io"
	"oxia/internal/client"
	"oxia/oxia"
	"time"
)

type BatcherFactory struct {
	Executor client.Executor
	Linger   time.Duration
	MaxSize  int
}

func (b *BatcherFactory) NewWriteBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, writeBatchFactory{execute: b.Executor.ExecuteWrite}.newBatch)
}

func (b *BatcherFactory) NewReadBatcher(shardId *uint32) Batcher {
	return b.newBatcher(shardId, readBatchFactory{execute: b.Executor.ExecuteRead}.newBatch)
}

func (b *BatcherFactory) newBatcher(shardId *uint32, batchFactory func(shardId *uint32) batch) Batcher {
	batcher := &batcherImpl{
		shardId:      shardId,
		batchFactory: batchFactory,
		c:            make(chan any, b.MaxSize), //TODO maybe unbuffered?
		close:        make(chan bool),
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
	batchFactory func(shardId *uint32) batch
	c            chan any
	close        chan bool
	linger       time.Duration
	maxSize      int
}

func (b *batcherImpl) Close() error {
	close(b.close)
	return nil
}

func (b *batcherImpl) Add(call any) {
	b.c <- call
}

func (b *batcherImpl) run() {
	var batch batch
	var timer *time.Timer = nil
	var timeout <-chan time.Time = nil
	for {
		select {
		case call := <-b.c:
			if batch == nil {
				batch = b.batchFactory(b.shardId)
				timer = time.NewTimer(b.linger)
				timeout = timer.C
			}
			batch.add(call)
			if batch.size() == b.maxSize {
				timer.Stop()
				go batch.complete()
				batch = nil
			}
		case <-timeout:
			timer.Stop()
			go batch.complete()
			batch = nil
		case <-b.close:
			if batch != nil {
				timer.Stop()
				batch.fail(oxia.ErrorShuttingDown)
				batch = nil
			}
			return
		}
	}
}
