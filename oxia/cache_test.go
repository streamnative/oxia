// Copyright 2023-2026 The Oxia Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
)

// Serves the records from memory. The gets and the puts fail with getErr and
// putErr, if set.
type recordsClient struct {
	SyncClient

	sync.Mutex
	records map[string][]byte
	gets    atomic.Int64
	// Runs once, in the next Get, after it read the record
	afterNextRead func()
	getErr        error
	putErr        error
}

func (r *recordsClient) Put(_ context.Context, key string, value []byte, _ ...PutOption) (string, Version, error) {
	r.Lock()
	defer r.Unlock()

	if r.putErr != nil {
		return "", Version{}, r.putErr
	}
	r.records[key] = value
	return key, Version{}, nil
}

func (r *recordsClient) Delete(_ context.Context, key string, _ ...DeleteOption) error {
	r.Lock()
	defer r.Unlock()

	delete(r.records, key)
	return nil
}

func (r *recordsClient) Get(_ context.Context, key string, _ ...GetOption) (string, []byte, Version, error) {
	r.gets.Add(1)

	r.Lock()
	value, found := r.records[key]
	afterRead := r.afterNextRead
	r.afterNextRead = nil
	getErr := r.getErr
	r.Unlock()

	if getErr != nil {
		return "", nil, Version{}, getErr
	}

	if afterRead != nil {
		afterRead()
	}
	if !found {
		return "", nil, Version{}, ErrKeyNotFound
	}
	return key, value, Version{}, nil
}

// Changes the record as another client would.
func (r *recordsClient) write(key string, value *string) {
	if value == nil {
		_ = r.Delete(context.Background(), key)
		return
	}
	data, _ := json.Marshal(*value)
	_, _, _ = r.Put(context.Background(), key, data)
}

// Runs change while a Get of the cache is loading "/key", after the server read
// the record, and returns once the load is done.
func loadRacing(t *testing.T, client *recordsClient, change func(c *cacheImpl[string])) *cacheImpl[string] {
	t.Helper()

	c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, c.Close())
	})

	read := make(chan struct{})
	release := make(chan struct{})
	client.afterNextRead = func() {
		close(read)
		<-release
	}

	loaded := make(chan struct{})
	go func() {
		defer close(loaded)
		_, _, _ = c.Get(context.Background(), "/key")
	}()

	<-read
	change(c)
	close(release)
	<-loaded
	return c
}

func assertRecord(t *testing.T, c *cacheImpl[string], expected *string) {
	t.Helper()

	value, _, err := c.Get(context.Background(), "/key")
	if expected == nil {
		assert.ErrorIs(t, err, ErrKeyNotFound, "value: %q", value)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, *expected, value)
	}

	c.loadsMutex.Lock()
	defer c.loadsMutex.Unlock()
	assert.Empty(t, c.loads)
}

// A notification that evicts a key while a Get is loading it from the server
// must not let the load cache the record as it was before the change.
func TestCache_LoadRacingNotification(t *testing.T) {
	v1, v2 := "v1", "v2"
	for _, test := range []struct {
		name         string
		before       *string
		after        *string
		notification Notification
	}{
		{"created", nil, &v1, Notification{Type: KeyCreated, Key: "/key"}},
		{"modified", &v1, &v2, Notification{Type: KeyModified, Key: "/key"}},
		{"deleted", &v1, nil, Notification{Type: KeyDeleted, Key: "/key"}},
		{"range-deleted", &v1, nil, Notification{Type: KeyRangeRangeDeleted, Key: "/a", KeyRangeEnd: "/z"}},
		{"missed", &v1, &v2, Notification{Type: NotificationsMissed, VersionId: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordsClient{records: map[string][]byte{}}
			client.write("/key", test.before)

			c := loadRacing(t, client, func(c *cacheImpl[string]) {
				client.write("/key", test.after)
				c.handleNotification(&test.notification)
			})
			assertRecord(t, c, test.after)
		})
	}
}

// Nor must a Put or a Delete through the cache.
func TestCache_LoadRacingWrite(t *testing.T) {
	v1, v2 := "v1", "v2"

	t.Run("put", func(t *testing.T) {
		client := &recordsClient{records: map[string][]byte{}}
		client.write("/key", &v1)

		c := loadRacing(t, client, func(c *cacheImpl[string]) {
			_, _, err := c.Put(context.Background(), "/key", v2)
			assert.NoError(t, err)
		})
		assertRecord(t, c, &v2)
	})

	t.Run("delete", func(t *testing.T) {
		client := &recordsClient{records: map[string][]byte{}}
		client.write("/key", &v1)

		c := loadRacing(t, client, func(c *cacheImpl[string]) {
			assert.NoError(t, c.Delete(context.Background(), "/key"))
		})
		assertRecord(t, c, nil)
	})
}

// A Get after a Put or a Delete through the cache returns the written record,
// also when a Get loaded the key just before: ristretto buffers the Set of the
// load, which must not be applied after the eviction of the write.
func TestCacheGetAfterWrite(t *testing.T) {
	const keys = 2_000
	v1, v2 := "v1", "v2"

	t.Run("put", func(t *testing.T) {
		client := &recordsClient{records: map[string][]byte{}}
		c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
		require.NoError(t, err)
		defer c.Close()

		for i := range keys {
			key := fmt.Sprintf("/key-%d", i)
			if i%2 == 0 {
				client.write(key, &v1)
			}
			_, _, _ = c.Get(context.Background(), key)
			_, _, err := c.Put(context.Background(), key, v2)
			require.NoError(t, err)

			value, _, err := c.Get(context.Background(), key)
			require.NoError(t, err, key)
			require.Equal(t, v2, value, key)
		}
	})

	t.Run("delete", func(t *testing.T) {
		client := &recordsClient{records: map[string][]byte{}}
		c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
		require.NoError(t, err)
		defer c.Close()

		for i := range keys {
			key := fmt.Sprintf("/key-%d", i)
			client.write(key, &v1)
			_, _, err := c.Get(context.Background(), key)
			require.NoError(t, err)
			require.NoError(t, c.Delete(context.Background(), key))

			_, _, err = c.Get(context.Background(), key)
			require.ErrorIs(t, err, ErrKeyNotFound, key)
		}
	})
}

// A Get that loads a key while the cache closes returns.
func TestCacheGetLoadingWhileClosing(t *testing.T) {
	v1 := "v1"
	client := &recordsClient{records: map[string][]byte{}}
	client.write("/key", &v1)
	c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
	require.NoError(t, err)

	read := make(chan struct{})
	release := make(chan struct{})
	client.afterNextRead = func() {
		close(read)
		<-release
	}
	loaded := make(chan error, 1)
	go func() {
		_, _, err := c.Get(context.Background(), "/key")
		loaded <- err
	}()

	<-read
	require.NoError(t, c.Close())
	close(release)
	select {
	case err := <-loaded:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the Get did not return")
	}
}

// The notification of another key must not keep a load from caching the record.
func TestCache_LoadRacingOtherKeyNotification(t *testing.T) {
	v1 := "v1"
	client := &recordsClient{records: map[string][]byte{}}
	client.write("/key", &v1)

	c := loadRacing(t, client, func(c *cacheImpl[string]) {
		client.write("/other", &v1)
		c.handleNotification(&Notification{Type: KeyCreated, Key: "/other"})
	})
	assertRecord(t, c, &v1)
	assert.EqualValues(t, 1, client.gets.Load(), "the second Get read the server")
}

func TestNewCacheRequiresTheClientImplementation(t *testing.T) {
	_, err := NewCache[string](&recordsClient{}, json.Marshal, json.Unmarshal)
	assert.ErrorContains(t, err, "Invalid client implementation")
}

func TestNewCacheFailsWithoutNotifications(t *testing.T) {
	errNotifications := errors.New("notifications not enabled")
	client := newSyncClient(&neverCompleteAsyncClient{notificationsErr: errNotifications})

	_, err := NewCache[string](client, json.Marshal, json.Unmarshal)
	assert.ErrorIs(t, err, errNotifications)
	assert.NoError(t, client.Close())
}

// The caches of a client share its subscription to the notifications.
func TestNewCacheSharesTheNotifications(t *testing.T) {
	notifications := &testNotifications{ch: make(chan *Notification)}
	asyncClient := &neverCompleteAsyncClient{notifications: notifications}
	client := newSyncClient(asyncClient)

	_, err := NewCache[string](client, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	_, err = NewCache[int](client, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	assert.EqualValues(t, 1, asyncClient.notificationsRequests.Load())

	assert.NoError(t, client.Close())
	assert.True(t, notifications.closed.Load())
}

func TestNewCacheFailsWhenTheValueCacheFails(t *testing.T) {
	injected := errors.New("injected")
	notifications := &testNotifications{ch: make(chan *Notification)}
	cm, err := newCacheManager(newSyncClient(&neverCompleteAsyncClient{notifications: notifications}),
		failpoint.New(map[string]func() error{
			"newCacheImpl.newCache": func() error { return injected },
		}))
	require.NoError(t, err)

	_, err = newCache[string](cm, json.Marshal, json.Unmarshal)

	assert.ErrorIs(t, err, injected)
	assert.Empty(t, cm.caches)
	assert.NoError(t, cm.Close())
}

// The cache manager stops once the notifications end.
func TestCacheManagerStopsWhenNotificationsEnd(t *testing.T) {
	notifications := &testNotifications{ch: make(chan *Notification)}
	cm, err := newCacheManager(newSyncClient(&neverCompleteAsyncClient{notifications: notifications}), nil)
	require.NoError(t, err)

	close(notifications.ch)
	assertGoroutinesEnd(t, "newCacheManager")
	assert.NoError(t, cm.Close())
}

func TestCacheManagerHandleNotificationLogsAtDebugLevel(t *testing.T) {
	logs := captureDebugLogs(t)
	client := &recordsClient{records: map[string][]byte{}}
	c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	defer func() { assert.NoError(t, c.Close()) }()
	value := "v1"
	client.write("/key", &value)
	_, _, err = c.Get(context.Background(), "/key")
	require.NoError(t, err)
	cm := &cacheManager{ctx: context.Background(), caches: []internalCache{c}}

	value = "v2"
	client.write("/key", &value)
	cm.handleNotification(&Notification{Type: KeyModified, Key: "/key", VersionId: 1})

	assert.Contains(t, logs.messages(), "Received notification")
	cached, _, err := c.Get(context.Background(), "/key")
	require.NoError(t, err)
	assert.Equal(t, "v2", cached)
}

func TestCachePutFailsOnSerializationError(t *testing.T) {
	errSerialize := errors.New("unsupported value")
	client := &recordsClient{records: map[string][]byte{}}
	c, err := newCacheImpl[string](client, nil, func(any) ([]byte, error) { return nil, errSerialize }, json.Unmarshal)
	require.NoError(t, err)
	defer func() { assert.NoError(t, c.Close()) }()

	_, _, err = c.Put(context.Background(), "/key", "v1")
	assert.ErrorIs(t, err, errSerialize)
	assert.Empty(t, client.records)
}

// A key that does not exist is cached as such.
func TestCacheGetCachesKeyNotFound(t *testing.T) {
	client := &recordsClient{records: map[string][]byte{}}
	c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	defer func() { assert.NoError(t, c.Close()) }()

	_, _, err = c.Get(context.Background(), "/key")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	c.valueCache.Wait()

	_, _, err = c.Get(context.Background(), "/key")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.EqualValues(t, 1, client.gets.Load(), "the second Get read the server")
}

// A failed read is not cached.
func TestCacheGetFailsOnReadError(t *testing.T) {
	errRead := errors.New("read failed")
	client := &recordsClient{records: map[string][]byte{}, getErr: errRead}
	c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	defer func() { assert.NoError(t, c.Close()) }()

	_, _, err = c.Get(context.Background(), "/key")
	assert.ErrorIs(t, err, errRead)
	c.valueCache.Wait()

	_, _, err = c.Get(context.Background(), "/key")
	assert.ErrorIs(t, err, errRead)
	assert.EqualValues(t, 2, client.gets.Load())
}

func TestCacheReadModifyUpdateCreatesMissingRecord(t *testing.T) {
	client := &recordsClient{records: map[string][]byte{}}
	c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	defer func() { assert.NoError(t, c.Close()) }()

	err = c.ReadModifyUpdate(context.Background(), "/key", func(v Optional[string]) (string, error) {
		assert.True(t, v.Empty())
		return "v1", nil
	})
	require.NoError(t, err)

	client.Lock()
	defer client.Unlock()
	assert.JSONEq(t, `"v1"`, string(client.records["/key"]))
}

func TestCacheReadModifyUpdateFailsWithoutRetrying(t *testing.T) {
	errFailure := errors.New("failure")
	for _, test := range []struct {
		name   string
		getErr error
		putErr error
		modify func(Optional[string]) (string, error)
	}{
		{"read-error", errFailure, nil, func(Optional[string]) (string, error) { return "v1", nil }},
		{"modify-error", nil, nil, func(Optional[string]) (string, error) { return "", errFailure }},
		{"write-error", nil, errFailure, func(Optional[string]) (string, error) { return "v1", nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordsClient{records: map[string][]byte{}, getErr: test.getErr, putErr: test.putErr}
			c, err := newCacheImpl[string](client, nil, json.Marshal, json.Unmarshal)
			require.NoError(t, err)
			defer func() { assert.NoError(t, c.Close()) }()

			attempts := 0
			err = c.ReadModifyUpdate(context.Background(), "/key", func(v Optional[string]) (string, error) {
				attempts++
				return test.modify(v)
			})
			assert.ErrorIs(t, err, errFailure)
			assert.LessOrEqual(t, attempts, 1)
			assert.EqualValues(t, 1, client.gets.Load())
		})
	}
}

// testNotifications delivers the notifications that the test sends on ch.
type testNotifications struct {
	ch     chan *Notification
	closed atomic.Bool
}

func (n *testNotifications) Ch() <-chan *Notification { return n.ch }

func (n *testNotifications) Close() error {
	n.closed.Store(true)
	return nil
}

// debugLogs records the messages of all the log records, at every level.
type debugLogs struct {
	sync.Mutex
	logged []string
}

func (*debugLogs) Enabled(context.Context, slog.Level) bool { return true }

func (l *debugLogs) Handle(_ context.Context, record slog.Record) error {
	l.Lock()
	defer l.Unlock()
	l.logged = append(l.logged, record.Message)
	return nil
}

func (l *debugLogs) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *debugLogs) WithGroup(string) slog.Handler { return l }

func (l *debugLogs) messages() []string {
	l.Lock()
	defer l.Unlock()
	return append([]string(nil), l.logged...)
}

// captureDebugLogs makes the default logger record its messages, until the test ends.
func captureDebugLogs(t *testing.T) *debugLogs {
	t.Helper()
	logs := &debugLogs{}
	previous := slog.Default()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}
