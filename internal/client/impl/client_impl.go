package impl

import (
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"oxia/common"
	"oxia/internal/client"
	"oxia/internal/client/batch"
	"oxia/oxia"
	"sync"
)

type clientImpl struct {
	sync.Mutex
	shardManager      client.ShardManager
	writeBatchManager *batch.Manager
	readBatchManager  *batch.Manager
}

func NewClient(options *oxia.Options) oxia.Client {
	clientPool := common.NewClientPool()
	shardManager := client.NewShardManager(client.NewShardStrategy(), clientPool, options.ServiceUrl)
	defer shardManager.Start()
	executor := &client.ExecutorImpl{
		ClientPool:   clientPool,
		ShardManager: shardManager,
		ServiceUrl:   options.ServiceUrl,
		Timeout:      options.BatchTimeout,
	}
	batcherFactory := &batch.BatcherFactory{
		Executor: executor,
		Linger:   options.BatchLinger,
		MaxSize:  options.BatchMaxSize,
	}
	return &clientImpl{
		shardManager:      shardManager,
		writeBatchManager: batch.NewManager(batcherFactory.NewWriteBatcher),
		readBatchManager:  batch.NewManager(batcherFactory.NewReadBatcher),
	}
}

func (c *clientImpl) Close() error {
	writeErr := c.writeBatchManager.Close()
	readErr := c.readBatchManager.Close()
	return multierr.Append(writeErr, readErr)
}

func (c *clientImpl) Put(key string, payload []byte, expectedVersion *int64) <-chan oxia.PutResult {
	ch := make(chan oxia.PutResult, 1)
	shardId := c.shardManager.Get(key)
	c.writeBatchManager.Get(shardId).Add(batch.PutCall{
		Key:             key,
		Payload:         payload,
		ExpectedVersion: expectedVersion,
		C:               ch,
	})
	return ch
}

func (c *clientImpl) Delete(key string, expectedVersion *int64) <-chan error {
	ch := make(chan error, 1)
	shardId := c.shardManager.Get(key)
	c.writeBatchManager.Get(shardId).Add(batch.DeleteCall{
		Key:             key,
		ExpectedVersion: expectedVersion,
		C:               ch,
	})
	return ch
}

func (c *clientImpl) DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error {
	shardIds := c.shardManager.GetAll()
	ch := make(chan error, 1)
	var eg errgroup.Group
	for _, shardId := range shardIds {
		cInner := make(chan error, 1)
		c.writeBatchManager.Get(shardId).Add(batch.DeleteRangeCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			C:               cInner,
		})
		eg.Go(func() error {
			return <-cInner
		})
	}
	go func() {
		ch <- eg.Wait()
		close(ch)
	}()
	return ch
}

func (c *clientImpl) Get(key string) <-chan oxia.GetResult {
	ch := make(chan oxia.GetResult, 1)
	shardId := c.shardManager.Get(key)
	c.readBatchManager.Get(shardId).Add(batch.GetCall{
		Key: key,
		C:   ch,
	})
	return ch
}

func (c *clientImpl) GetRange(minKeyInclusive string, maxKeyExclusive string) <-chan oxia.GetRangeResult {
	shardIds := c.shardManager.GetAll()
	ch := make(chan oxia.GetRangeResult, 1)
	var wg sync.WaitGroup
	wg.Add(len(shardIds))
	keys := make([]string, 0)
	for _, shardId := range shardIds {
		cInner := make(chan oxia.GetRangeResult, 1)
		c.readBatchManager.Get(shardId).Add(batch.GetRangeCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			C:               cInner,
		})
		go func() {
			x := <-cInner
			keys = append(keys, x.Keys...)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		ch <- oxia.GetRangeResult{
			Keys: keys,
		}
		close(ch)
	}()
	return ch
}
