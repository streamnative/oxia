package oxia

type syncClientImpl struct {
	asyncClient AsyncClient
}

func NewSyncClient(options ClientOptions) SyncClient {
	options.BatchMaxSize = 1
	return newSyncClient(NewAsyncClient(options))
}

func newSyncClient(asyncClient AsyncClient) SyncClient {
	return &syncClientImpl{
		asyncClient: asyncClient,
	}
}

func (c *syncClientImpl) Close() error {
	return c.asyncClient.Close()
}

func (c *syncClientImpl) Put(key string, payload []byte, expectedVersionId *int64) (Version, error) {
	r := <-c.asyncClient.Put(key, payload, expectedVersionId)
	return r.Version, r.Err
}

func (c *syncClientImpl) Delete(key string, expectedVersionId *int64) error {
	r := <-c.asyncClient.Delete(key, expectedVersionId)
	return r
}

func (c *syncClientImpl) DeleteRange(minKeyInclusive string, maxKeyExclusive string) error {
	r := <-c.asyncClient.DeleteRange(minKeyInclusive, maxKeyExclusive)
	return r
}

func (c *syncClientImpl) Get(key string) ([]byte, Version, error) {
	r := <-c.asyncClient.Get(key)
	return r.Payload, r.Version, r.Err
}

func (c *syncClientImpl) GetRange(minKeyInclusive string, maxKeyExclusive string) ([]string, error) {
	r := <-c.asyncClient.GetRange(minKeyInclusive, maxKeyExclusive)
	return r.Keys, r.Err
}
