package impl

import (
	"oxia/common"
	"oxia/oxia"
)

type memoryClient struct {
	clock common.Clock
	data  map[string]oxia.Value
}

func NewMemoryClient() oxia.Client {
	return newMemoryClientWithClock(common.SystemClock())
}

func newMemoryClientWithClock(clock common.Clock) oxia.Client {
	return &memoryClient{
		clock: clock,
		data:  make(map[string]oxia.Value),
	}
}

func (c *memoryClient) Close() error {
	return nil
}

func (c *memoryClient) Put(key string, payload []byte, expectedVersion *int64) <-chan oxia.PutResult {
	ch := make(chan oxia.PutResult, 1)
	now := c.clock.NowMillis()
	if value, ok := c.data[key]; ok {
		if expectedVersion != nil && *expectedVersion != value.Stat.Version {
			ch <- oxia.PutResult{Err: oxia.ErrorBadVersion}
		} else {
			value.Payload = payload
			value.Stat.Version = value.Stat.Version + 1
			value.Stat.ModifiedTimestamp = now
			ch <- oxia.PutResult{
				Stat: value.Stat,
			}
		}
	} else {
		if expectedVersion != nil && *expectedVersion != oxia.VersionNotExists {
			ch <- oxia.PutResult{Err: oxia.ErrorBadVersion}
		} else {
			value = oxia.Value{
				Payload: payload,
				Stat: oxia.Stat{
					Version:           1,
					CreatedTimestamp:  now,
					ModifiedTimestamp: now,
				},
			}
			c.data[key] = value
			ch <- oxia.PutResult{
				Stat: value.Stat,
			}
		}
	}
	close(ch)
	return ch
}

func (c *memoryClient) Delete(key string, expectedVersion *int64) <-chan error {
	ch := make(chan error, 1)
	if value, ok := c.data[key]; ok {
		if expectedVersion != nil && *expectedVersion != value.Stat.Version {
			ch <- oxia.ErrorBadVersion
		} else {
			delete(c.data, key)
		}
	} else {
		ch <- oxia.ErrorKeyNotFound
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

func (c *memoryClient) Get(key string) <-chan oxia.GetResult {
	ch := make(chan oxia.GetResult, 1)
	if value, ok := c.data[key]; ok {
		ch <- oxia.GetResult{
			Value: value,
		}
	} else {
		ch <- oxia.GetResult{
			Err: oxia.ErrorKeyNotFound,
		}
	}
	close(ch)
	return ch
}

func (c *memoryClient) GetRange(minKeyInclusive string, maxKeyExclusive string) <-chan oxia.GetRangeResult {
	ch := make(chan oxia.GetRangeResult, 1)
	result := make([]string, 0)
	for key := range c.data {
		if minKeyInclusive <= key && key < maxKeyExclusive {
			result = append(result, key)
		}
	}
	ch <- oxia.GetRangeResult{
		Keys: result,
	}
	close(ch)
	return ch
}
