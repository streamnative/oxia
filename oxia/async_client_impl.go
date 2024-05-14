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

package oxia

import (
	"context"
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/common/compare"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"io"
	"sync"

	"github.com/streamnative/oxia/common"
	commonbatch "github.com/streamnative/oxia/common/batch"
	"github.com/streamnative/oxia/oxia/internal"
	"github.com/streamnative/oxia/oxia/internal/batch"
	"github.com/streamnative/oxia/oxia/internal/metrics"
	"github.com/streamnative/oxia/oxia/internal/model"
	"github.com/streamnative/oxia/proto"
)

type clientImpl struct {
	sync.Mutex
	options           clientOptions
	shardManager      internal.ShardManager
	writeBatchManager *batch.Manager
	readBatchManager  *batch.Manager
	executor          internal.Executor
	sessions          *sessions
	notifications     []*notifications

	clientPool common.ClientPool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewAsyncClient creates a new Oxia client with the async interface
//
// ServiceAddress is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
// shard assignments. Ideally this should be a load-balanced endpoint.
//
// A list of ClientOption arguments can be passed to configure the Oxia client.
// Example:
//
//	client, err := oxia.NewAsyncClient("my-oxia-service:6648", oxia.WithBatchLinger(10*time.Milliseconds))
func NewAsyncClient(serviceAddress string, opts ...ClientOption) (AsyncClient, error) {
	options, err := newClientOptions(serviceAddress, opts...)
	if err != nil {
		return nil, err
	}

	clientPool := common.NewClientPool(options.tls)

	shardManager, err := internal.NewShardManager(internal.NewShardStrategy(), clientPool, serviceAddress,
		options.namespace, options.requestTimeout)
	if err != nil {
		return nil, err
	}

	executor := &internal.ExecutorImpl{
		ClientPool:     clientPool,
		ShardManager:   shardManager,
		ServiceAddress: options.serviceAddress,
	}
	batcherFactory := batch.NewBatcherFactory(
		executor,
		options.namespace,
		options.batchLinger,
		options.maxRequestsPerBatch,
		metrics.NewMetrics(options.meterProvider),
		options.requestTimeout)
	c := &clientImpl{
		options:      options,
		clientPool:   clientPool,
		shardManager: shardManager,
		writeBatchManager: batch.NewManager(func(shard *int64) commonbatch.Batcher {
			return batcherFactory.NewWriteBatcher(shard, options.maxBatchSize)
		}),
		readBatchManager: batch.NewManager(batcherFactory.NewReadBatcher),
		executor:         executor,
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.sessions = newSessions(c.ctx, c.shardManager, c.clientPool, c.options)
	return c, nil
}

func (c *clientImpl) Close() error {
	err := multierr.Combine(
		c.sessions.Close(),
		c.writeBatchManager.Close(),
		c.readBatchManager.Close(),
		c.clientPool.Close(),
	)
	c.cancel()

	err = multierr.Append(err, c.closeNotifications())
	return err
}

func (c *clientImpl) Put(key string, value []byte, options ...PutOption) <-chan PutResult {
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
	opts := newPutOptions(options)
	putCall := model.PutCall{
		Key:               key,
		Value:             value,
		ExpectedVersionId: opts.expectedVersion,
		Callback:          callback,
	}
	if opts.ephemeral {
		putCall.ClientIdentity = &c.options.identity
		c.sessions.executeWithSessionId(shardId, func(sessionId int64, err error) {
			if err != nil {
				callback(nil, err)
				return
			}
			putCall.SessionId = &sessionId
			c.writeBatchManager.Get(shardId).Add(putCall)
		})
	} else {
		c.writeBatchManager.Get(shardId).Add(putCall)
	}
	return ch
}

func (c *clientImpl) Delete(key string, options ...DeleteOption) <-chan error {
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
	opts := newDeleteOptions(options)
	c.writeBatchManager.Get(shardId).Add(model.DeleteCall{
		Key:               key,
		ExpectedVersionId: opts.expectedVersion,
		Callback:          callback,
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

func (c *clientImpl) Get(key string, options ...GetOption) <-chan GetResult {
	ch := make(chan GetResult)

	opts := newGetOptions(options)
	if opts.comparisonType == proto.KeyComparisonType_EQUAL {
		c.doExactGet(key, ch)
	} else {
		c.doFloorCeilingGet(key, opts.comparisonType, ch)
	}

	return ch
}

func (c *clientImpl) doExactGet(key string, ch chan GetResult) {
	shardId := c.shardManager.Get(key)
	c.readBatchManager.Get(shardId).Add(model.GetCall{
		Key: key,
		Callback: func(response *proto.GetResponse, err error) {
			ch <- toGetResult(response, key, err)
			close(ch)
		},
	})
}

// The keys might get hashed to multiple shards, so we have to check on all shards and then compare the results
func (c *clientImpl) doFloorCeilingGet(key string, comparisonType proto.KeyComparisonType, ch chan GetResult) {
	m := sync.Mutex{}
	var results []*proto.GetResponse
	shards := c.shardManager.GetAll()
	counter := len(shards)

	for _, shardId := range shards {
		c.readBatchManager.Get(shardId).Add(model.GetCall{
			Key:            key,
			ComparisonType: comparisonType,
			Callback: func(response *proto.GetResponse, err error) {
				m.Lock()
				defer m.Unlock()

				if err != nil {
					ch <- toGetResult(nil, key, err)
					close(ch)
					counter = 0
				}

				if response.Status == proto.Status_OK {
					results = append(results, response)
				}

				counter -= 1
				if counter == 0 {
					// We have responses from all the shards
					var selected *proto.GetResponse

					if len(results) == 0 {
						// We haven't found the key on any shard
						selected = &proto.GetResponse{
							Status: proto.Status_KEY_NOT_FOUND,
						}
					} else {
						slices.SortFunc(results, func(a, b *proto.GetResponse) bool {
							return compare.CompareWithSlash([]byte(a.GetKey()), []byte(b.GetKey())) < 0
						})

						switch comparisonType {
						case proto.KeyComparisonType_FLOOR:
							selected = results[len(results)-1]
						case proto.KeyComparisonType_LOWER:
							selected = results[len(results)-1]
						case proto.KeyComparisonType_CEILING:
							selected = results[0]
						case proto.KeyComparisonType_HIGHER:
							selected = results[0]
						}
					}

					ch <- toGetResult(selected, key, err)
					close(ch)
				}
			},
		})
	}
}

func (c *clientImpl) listFromShard(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, shardId int64, ch chan<- ListResult) {
	request := &proto.ListRequest{
		ShardId:        &shardId,
		StartInclusive: minKeyInclusive,
		EndExclusive:   maxKeyExclusive,
	}

	client, err := c.executor.ExecuteList(ctx, request)
	if err != nil {
		ch <- ListResult{Err: err}
		return
	}

	for {
		response, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			ch <- ListResult{Err: err}
			return
		}

		ch <- ListResult{Keys: response.Keys}
	}
}

func (c *clientImpl) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) <-chan ListResult {
	shardIds := c.shardManager.GetAll()
	ch := make(chan ListResult)
	wg := common.NewWaitGroup(len(shardIds))
	for _, shardId := range shardIds {
		shardIdPtr := shardId
		go func() {
			defer wg.Done()

			c.listFromShard(ctx, minKeyInclusive, maxKeyExclusive, shardIdPtr, ch)
		}()
	}

	go func() {
		_ = wg.Wait(ctx)
		close(ch)
	}()

	return ch
}

func (c *clientImpl) closeNotifications() error {
	c.Lock()
	defer c.Unlock()

	var err error
	for _, n := range c.notifications {
		err = multierr.Append(err, n.Close())
	}

	return err
}

func (c *clientImpl) GetNotifications() (Notifications, error) {
	nm, err := newNotifications(c.ctx, c.options, c.clientPool, c.shardManager)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create notification stream")
	}

	c.Lock()
	defer c.Unlock()
	c.notifications = append(c.notifications, nm)

	return nm, nil
}
