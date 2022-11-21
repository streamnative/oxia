package oxia

type syncClientImpl struct {
	asyncClient AsyncClient
}

func NewSyncClient(options ClientOptions) (SyncClient, error) {
	options.BatchLinger = 0
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if client, err := NewAsyncClient(options); err != nil {
		return nil, err
	} else {
		return newSyncClient(client), nil
	}
}

func newSyncClient(asyncClient AsyncClient) SyncClient {
	return &syncClientImpl{
		asyncClient: asyncClient,
	}
}

func (c *syncClientImpl) Close() error {
	return c.asyncClient.Close()
}

func (c *syncClientImpl) Put(key string, payload []byte, expectedVersion *int64) (Stat, error) {
	r := <-c.asyncClient.Put(key, payload, expectedVersion)
	return r.Stat, r.Err
}

func (c *syncClientImpl) Delete(key string, expectedVersion *int64) error {
	r := <-c.asyncClient.Delete(key, expectedVersion)
	return r
}

func (c *syncClientImpl) DeleteRange(minKeyInclusive string, maxKeyExclusive string) error {
	r := <-c.asyncClient.DeleteRange(minKeyInclusive, maxKeyExclusive)
	return r
}

func (c *syncClientImpl) Get(key string) ([]byte, Stat, error) {
	r := <-c.asyncClient.Get(key)
	return r.Payload, r.Stat, r.Err
}

func (c *syncClientImpl) GetRange(minKeyInclusive string, maxKeyExclusive string) ([]string, error) {
	r := <-c.asyncClient.GetRange(minKeyInclusive, maxKeyExclusive)
	return r.Keys, r.Err
}
