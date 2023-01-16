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
)

type syncClientImpl struct {
	asyncClient AsyncClient
}

// NewSyncClient creates a new Oxia client with the sync interface
//
// ServiceAddress is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
// shard assignments. Ideally this should be a load-balanced endpoint.
//
// A list of ClientOption arguments can be passed to configure the Oxia client
func NewSyncClient(serviceAddress string, opts ...ClientOption) (SyncClient, error) {
	options := append(opts, WithBatchLinger(0))

	asyncClient, err := NewAsyncClient(serviceAddress, options...)
	if err != nil {
		return nil, err
	}
	return newSyncClient(asyncClient), nil
}

func newSyncClient(asyncClient AsyncClient) SyncClient {
	return &syncClientImpl{
		asyncClient: asyncClient,
	}
}

func (c *syncClientImpl) Close() error {
	return c.asyncClient.Close()
}

func (c *syncClientImpl) Put(ctx context.Context, key string, payload []byte, expectedVersion *int64) (Stat, error) {
	select {
	case r := <-c.asyncClient.Put(key, payload, expectedVersion):
		return r.Stat, r.Err
	case <-ctx.Done():
		return Stat{}, ctx.Err()
	}
}

func (c *syncClientImpl) Delete(ctx context.Context, key string, expectedVersion *int64) error {
	select {
	case err := <-c.asyncClient.Delete(key, expectedVersion):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *syncClientImpl) DeleteRange(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) error {
	select {
	case err := <-c.asyncClient.DeleteRange(minKeyInclusive, maxKeyExclusive):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *syncClientImpl) Get(ctx context.Context, key string) ([]byte, Stat, error) {
	select {
	case r := <-c.asyncClient.Get(key):
		return r.Payload, r.Stat, r.Err
	case <-ctx.Done():
		return nil, Stat{}, ctx.Err()
	}
}

func (c *syncClientImpl) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) ([]string, error) {
	select {
	case r := <-c.asyncClient.List(minKeyInclusive, maxKeyExclusive):
		return r.Keys, r.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *syncClientImpl) GetNotifications() (Notifications, error) {
	return c.asyncClient.GetNotifications()
}
