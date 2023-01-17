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
	"oxia/common"
)

type memoryClient struct {
	clock common.Clock
	data  map[string]GetResult
}

func newMemoryClientWithClock(clock common.Clock) AsyncClient {
	return &memoryClient{
		clock: clock,
		data:  make(map[string]GetResult),
	}
}

func (c *memoryClient) Close() error {
	return nil
}

func (c *memoryClient) Put(key string, payload []byte, options ...PutOption) <-chan PutResult {
	ch := make(chan PutResult, 1)
	now := uint64(c.clock.Now().UnixMilli())
	opts := newPutOptions(options)
	if value, ok := c.data[key]; ok {
		if opts.expectedVersion != nil && *opts.expectedVersion != value.Stat.Version {
			ch <- PutResult{Err: ErrorUnexpectedVersion}
		} else {
			value.Payload = payload
			value.Stat.Version = value.Stat.Version + 1
			value.Stat.ModifiedTimestamp = now
			ch <- PutResult{
				Stat: value.Stat,
			}
		}
	} else {
		if opts.expectedVersion != nil && *opts.expectedVersion != VersionNotExists {
			ch <- PutResult{Err: ErrorUnexpectedVersion}
		} else {
			value = GetResult{
				Payload: payload,
				Stat: Stat{
					Version:           1,
					CreatedTimestamp:  now,
					ModifiedTimestamp: now,
				},
			}
			c.data[key] = value
			ch <- PutResult{
				Stat: value.Stat,
			}
		}
	}
	close(ch)
	return ch
}

func (c *memoryClient) Delete(key string, options ...DeleteOption) <-chan error {
	ch := make(chan error, 1)
	opts := newDeleteOptions(options)
	if value, ok := c.data[key]; ok {
		if opts.expectedVersion != nil && *opts.expectedVersion != value.Stat.Version {
			ch <- ErrorUnexpectedVersion
		} else {
			delete(c.data, key)
		}
	} else {
		ch <- ErrorKeyNotFound
	}
	close(ch)
	return ch
}

func (c *memoryClient) DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error {
	for key := range c.data {
		if minKeyInclusive <= key && key < maxKeyExclusive {
			delete(c.data, key)
		}
	}
	ch := make(chan error, 1)
	close(ch)
	return ch
}

func (c *memoryClient) Get(key string) <-chan GetResult {
	ch := make(chan GetResult, 1)
	if value, ok := c.data[key]; ok {
		ch <- value
	} else {
		ch <- GetResult{
			Err: ErrorKeyNotFound,
		}
	}
	close(ch)
	return ch
}

func (c *memoryClient) List(minKeyInclusive string, maxKeyExclusive string) <-chan ListResult {
	ch := make(chan ListResult, 1)
	result := make([]string, 0)
	for key := range c.data {
		if minKeyInclusive <= key && key < maxKeyExclusive {
			result = append(result, key)
		}
	}
	ch <- ListResult{
		Keys: result,
	}
	close(ch)
	return ch
}

type memoryNotifcationManager struct {
	ch chan *Notification
}

func (m memoryNotifcationManager) Ch() <-chan *Notification {
	return m.ch
}

func (m memoryNotifcationManager) Close() error {
	return nil
}

func (c *memoryClient) GetNotifications() (Notifications, error) {
	return &memoryNotifcationManager{make(chan *Notification)}, nil
}
