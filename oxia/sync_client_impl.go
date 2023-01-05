package oxia

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

func (c *syncClientImpl) List(minKeyInclusive string, maxKeyExclusive string) ([]string, error) {
	r := <-c.asyncClient.List(minKeyInclusive, maxKeyExclusive)
	return r.Keys, r.Err
}

func (c *syncClientImpl) GetNotifications() (Notifications, error) {
	return c.asyncClient.GetNotifications()
}
