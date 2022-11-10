package oxia

import "oxia/common"

type memoryClient struct {
	clock common.Clock
	data  map[string]Value
}

func NewMemoryClient() Client {
	return newMemoryClientWithClock(common.SystemClock())
}

func newMemoryClientWithClock(clock common.Clock) Client {
	return &memoryClient{
		clock: clock,
		data:  make(map[string]Value),
	}
}

func (c *memoryClient) Close() error {
	return nil
}

func (c *memoryClient) Put(key string, payload []byte, expectedVersion *int64) (Stat, error) {
	now := c.clock.NowMillis()
	if value, ok := c.data[key]; ok {
		if expectedVersion != nil && *expectedVersion != value.Stat.Version {
			return Stat{}, ErrorBadVersion
		}
		value.Payload = payload
		value.Stat.Version = value.Stat.Version + 1
		value.Stat.ModifiedTimestamp = now
		return value.Stat, nil
	} else {
		if expectedVersion != nil && *expectedVersion != VersionNotExists {
			return Stat{}, ErrorBadVersion
		}
		value = Value{
			Payload: payload,
			Stat: Stat{
				Version:           1,
				CreatedTimestamp:  now,
				ModifiedTimestamp: now,
			},
		}
		c.data[key] = value
		return value.Stat, nil
	}
}

func (c *memoryClient) Delete(key string, expectedVersion *int64) error {
	if value, ok := c.data[key]; ok {
		if expectedVersion != nil && *expectedVersion != value.Stat.Version {
			return ErrorBadVersion
		}
		delete(c.data, key)
		return nil
	} else {
		return ErrorKeyNotFound
	}
}

func (c *memoryClient) DeleteRange(minKeyInclusive string, maxKeyExclusive string) error {
	for key := range c.data {
		if minKeyInclusive <= key && key < maxKeyExclusive {
			_ = c.Delete(key, nil)
		}
	}
	return nil
}

func (c *memoryClient) Get(key string) (Value, error) {
	if value, ok := c.data[key]; ok {
		return value, nil
	} else {
		return Value{}, ErrorKeyNotFound
	}
}

func (c *memoryClient) GetRange(minKeyInclusive string, maxKeyExclusive string) ([]string, error) {
	result := make([]string, 0)
	for key := range c.data {
		if minKeyInclusive <= key && key < maxKeyExclusive {
			result = append(result, key)
		}
	}
	return result, nil
}
