package oxia

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

type neverCompleteAsyncClient struct {
}

func (c *neverCompleteAsyncClient) Close() error { return nil }

func (c *neverCompleteAsyncClient) Put(key string, payload []byte, expectedVersion *int64) <-chan PutResult {
	return make(chan PutResult)
}

func (c *neverCompleteAsyncClient) Delete(key string, expectedVersion *int64) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) Get(key string) <-chan GetResult {
	return make(chan GetResult)
}

func (c *neverCompleteAsyncClient) List(minKeyInclusive string, maxKeyExclusive string) <-chan ListResult {
	return make(chan ListResult)
}

func (c *neverCompleteAsyncClient) GetNotifications() (Notifications, error) {
	panic("not implemented")
}

func TestCancelContext(t *testing.T) {
	_asyncClient := &neverCompleteAsyncClient{}
	syncClient := newSyncClient(_asyncClient)

	assertCancellable(t, func(ctx context.Context) error {
		_, err := syncClient.Put(ctx, "/a", []byte{}, nil)
		return err
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.Delete(ctx, "/a", nil)
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.DeleteRange(ctx, "/a", "/b")
	})
	assertCancellable(t, func(ctx context.Context) error {
		_, _, err := syncClient.Get(ctx, "/a")
		return err
	})
	assertCancellable(t, func(ctx context.Context) error {
		_, err := syncClient.List(ctx, "/a", "/b")
		return err
	})

	err := syncClient.Close()
	assert.NoError(t, err)
}

func assertCancellable(t *testing.T, operationFunc func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error)
	go func() {
		errCh <- operationFunc(ctx)
	}()

	cancel()

	assert.ErrorIs(t, <-errCh, context.Canceled)
}
