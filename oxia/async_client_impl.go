package oxia

import (
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"oxia/common"
	"oxia/oxia/internal"
	"oxia/oxia/internal/batch"
	"oxia/oxia/internal/metrics"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"sync"
)

type clientImpl struct {
	sync.Mutex
	shardManager      internal.ShardManager
	writeBatchManager *batch.Manager
	readBatchManager  *batch.Manager
}

func NewAsyncClient(options ClientOptions) AsyncClient {
	clientPool := common.NewClientPool()
	shardManager := internal.NewShardManager(internal.NewShardStrategy(), clientPool, options.serviceAddress)
	defer shardManager.Start()
	executor := &internal.ExecutorImpl{
		ClientPool:     clientPool,
		ShardManager:   shardManager,
		ServiceAddress: options.serviceAddress,
		Timeout:        options.batchRequestTimeout,
	}
	batcherFactory := &batch.BatcherFactory{
		Executor:            executor,
		Linger:              options.batchLinger,
		MaxRequestsPerBatch: options.maxRequestsPerBatch,
		Metrics:             metrics.NewMetrics(options.meterProvider),
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

func (c *clientImpl) Put(key string, payload []byte, expectedVersion *int64) <-chan PutResult {
	ch := make(chan PutResult, 1)
	shardId := c.shardManager.Get(key)
	callback := func(response *proto.PutResponse, err error) {
		if err != nil {
			ch <- PutResult{Err: err}
		} else {
			ch <- toPutResult(response)
		}
		close(ch)
	}
	c.writeBatchManager.Get(shardId).Add(model.PutCall{
		Key:             key,
		Payload:         payload,
		ExpectedVersion: expectedVersion,
		Callback:        callback,
	})
	return ch
}

func (c *clientImpl) Delete(key string, expectedVersion *int64) <-chan error {
	ch := make(chan error, 1)
	shardId := c.shardManager.Get(key)
	callback := func(response *proto.DeleteResponse, err error) {
		if err != nil {
			ch <- err
		} else {
			ch <- toDeleteResult(response)
		}
		close(ch)
	}
	c.writeBatchManager.Get(shardId).Add(model.DeleteCall{
		Key:             key,
		ExpectedVersion: expectedVersion,
		Callback:        callback,
	})
	return ch
}

func (c *clientImpl) DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error {
	shardIds := c.shardManager.GetAll()
	ch := make(chan error, 1)
	var eg errgroup.Group
	for _, shardId := range shardIds {
		chInner := make(chan error, 1)
		callback := func(response *proto.DeleteRangeResponse, err error) {
			if err != nil {
				chInner <- err
			} else {
				chInner <- toDeleteRangeResult(response)
			}
		}
		c.writeBatchManager.Get(shardId).Add(model.DeleteRangeCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			Callback:        callback,
		})
		eg.Go(func() error {
			return <-chInner
		})
	}
	go func() {
		ch <- eg.Wait()
		close(ch)
	}()
	return ch
}

func (c *clientImpl) Get(key string) <-chan GetResult {
	ch := make(chan GetResult, 1)
	shardId := c.shardManager.Get(key)
	callback := func(response *proto.GetResponse, err error) {
		if err != nil {
			ch <- GetResult{Err: err}
		} else {
			ch <- toGetResult(response)
		}
		close(ch)
	}
	c.readBatchManager.Get(shardId).Add(model.GetCall{
		Key:      key,
		Callback: callback,
	})
	return ch
}

func (c *clientImpl) GetRange(minKeyInclusive string, maxKeyExclusive string) <-chan GetRangeResult {
	shardIds := c.shardManager.GetAll()
	ch := make(chan GetRangeResult, 1)
	var wg sync.WaitGroup
	wg.Add(len(shardIds))
	keys := make([]string, 0)
	for _, shardId := range shardIds {
		cInner := make(chan GetRangeResult, 1)
		callback := func(response *proto.GetRangeResponse, err error) {
			if err != nil {
				cInner <- GetRangeResult{Err: err}
			} else {
				cInner <- toGetRangeResult(response)
			}
		}
		c.readBatchManager.Get(shardId).Add(model.GetRangeCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			Callback:        callback,
		})
		go func() {
			x := <-cInner
			keys = append(keys, x.Keys...)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		ch <- GetRangeResult{
			Keys: keys,
		}
		close(ch)
	}()
	return ch
}
