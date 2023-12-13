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
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/server"
)

func init() {
	common.ConfigureLogger()
}

func TestAsyncClientImpl(t *testing.T) {
	standaloneServer, err := server.NewStandalone(server.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", standaloneServer.RpcPort())
	client, err := NewAsyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	putResultA := <-client.Put("/a", []byte{0}, ExpectedRecordNotExists())
	assert.EqualValues(t, 0, putResultA.Version.VersionId)
	assert.EqualValues(t, 0, putResultA.Version.ModificationsCount)

	getResult := <-client.Get("/a")
	assert.Equal(t, GetResult{
		Value:   []byte{0},
		Version: putResultA.Version,
	}, getResult)

	putResultC1 := <-client.Put("/c", []byte{0}, ExpectedRecordNotExists())
	assert.EqualValues(t, 1, putResultC1.Version.VersionId)
	assert.EqualValues(t, 0, putResultC1.Version.ModificationsCount)

	putResultC2 := <-client.Put("/c", []byte{1}, ExpectedVersionId(putResultC1.Version.VersionId))
	assert.EqualValues(t, 2, putResultC2.Version.VersionId)
	assert.EqualValues(t, 1, putResultC2.Version.ModificationsCount)

	listResult := <-client.List(context.Background(), "/y", "/z")
	assert.Len(t, listResult.Keys, 0)

	listResult = <-client.List(context.Background(), "/a", "/d")
	assert.Equal(t, ListResult{
		Keys: []string{"/a", "/c"},
	}, listResult)

	deleteErr := <-client.Delete("/a", ExpectedVersionId(putResultA.Version.VersionId))
	assert.NoError(t, deleteErr)

	getResult = <-client.Get("/a")
	assert.Equal(t, GetResult{
		Err: ErrKeyNotFound,
	}, getResult)

	deleteRangeResult := <-client.DeleteRange("/c", "/d")
	assert.NoError(t, deleteRangeResult)

	getResult = <-client.Get("/d")
	assert.Equal(t, GetResult{
		Err: ErrKeyNotFound,
	}, getResult)

	err = client.Close()
	assert.NoError(t, err)

	err = standaloneServer.Close()
	assert.NoError(t, err)
}

func TestSyncClientImpl_Notifications(t *testing.T) {
	standaloneServer, err := server.NewStandalone(server.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", standaloneServer.RpcPort())
	client, err := NewSyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	notifications, err := client.GetNotifications()
	assert.NoError(t, err)

	ctx := context.Background()

	s1, _ := client.Put(ctx, "/a", []byte("0"))

	n := <-notifications.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s1.VersionId, n.VersionId)

	s2, _ := client.Put(ctx, "/a", []byte("1"))

	n = <-notifications.Ch()
	assert.Equal(t, KeyModified, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s2.VersionId, n.VersionId)

	s3, _ := client.Put(ctx, "/b", []byte("0"))
	assert.NoError(t, client.Delete(ctx, "/a"))

	n = <-notifications.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/b", n.Key)
	assert.Equal(t, s3.VersionId, n.VersionId)

	n = <-notifications.Ch()
	assert.Equal(t, KeyDeleted, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.EqualValues(t, -1, n.VersionId)

	// Create a 2nd notifications channel
	// This will only receive new updates
	notifications2, err := client.GetNotifications()
	assert.NoError(t, err)

	select {
	case <-notifications2.Ch():
		assert.Fail(t, "shouldn't have received any notifications")
	case <-time.After(100 * time.Millisecond):
		// Ok, we expect it to time out
	}

	s4, _ := client.Put(ctx, "/x", []byte("1"))

	n = <-notifications.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.VersionId, n.VersionId)

	n = <-notifications2.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.VersionId, n.VersionId)

	assert.NoError(t, client.Close())

	// Channels should be closed after the client is closed
	select {
	case <-notifications.Ch():
		// Ok

	case <-time.After(1 * time.Second):
		assert.Fail(t, "should have been closed")
	}

	select {
	case <-notifications2.Ch():
		// Ok

	case <-time.After(1 * time.Second):
		assert.Fail(t, "should have been closed")
	}

	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_NotificationsClose(t *testing.T) {
	standaloneServer, err := server.NewStandalone(server.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", standaloneServer.RpcPort())
	client, err := NewSyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	notifications, err := client.GetNotifications()
	assert.NoError(t, err)

	assert.NoError(t, notifications.Close())

	select {
	case n := <-notifications.Ch():
		assert.Nil(t, n)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_Sessions(t *testing.T) {
	standaloneServer, err := server.NewStandalone(server.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", standaloneServer.RpcPort())
	client, err := NewAsyncClient(serviceAddress, WithBatchLinger(0), WithSessionTimeout(5*time.Second))
	assert.NoError(t, err)

	putCh := client.Put("/x", []byte("x"), Ephemeral())
	versionId := atomic.Int64{}

	select {
	case res := <-putCh:
		assert.NotNil(t, res)
		assert.NoError(t, res.Err)
		assert.EqualValues(t, 0, res.Version.ModificationsCount)
		versionId.Store(res.Version.VersionId)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}
	getCh := client.Get("/x")
	select {
	case res := <-getCh:
		assert.NotNil(t, res)
		assert.NoError(t, res.Err)
		assert.EqualValues(t, 0, res.Version.ModificationsCount)
		assert.Equal(t, versionId.Load(), res.Version.VersionId)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}
	assert.NoError(t, client.Close())
	slog.Debug("First client closed")

	client, err = NewAsyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		getCh = client.Get("/x")
		select {
		case res := <-getCh:
			assert.NotNil(t, res)
			slog.Debug(
				"Get resulted in",
				slog.Any("res", res),
			)
			return errors.Is(res.Err, ErrKeyNotFound)

		case <-time.After(1 * time.Second):
			assert.Fail(t, "Shouldn't have timed out")
			return false
		}
	}, 8*time.Second, 500*time.Millisecond)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_OverrideEphemeral(t *testing.T) {
	client, err := NewSyncClient(serviceAddress,
		WithSessionTimeout(5*time.Second),
	)
	assert.NoError(t, err)

	k := newKey()
	version, err := client.Put(context.Background(), k, []byte("v1"), Ephemeral())
	assert.NoError(t, err)

	assert.True(t, version.Ephemeral)

	// Override with non-ephemeral value
	version, err = client.Put(context.Background(), k, []byte("v2"))
	assert.NoError(t, err)

	assert.False(t, version.Ephemeral)
	assert.Equal(t, "", version.ClientIdentity)

	assert.NoError(t, client.Close())

	// Reopen
	client, err = NewSyncClient(serviceAddress)
	assert.NoError(t, err)

	var res []byte
	res, version, err = client.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, version.ModificationsCount)
	assert.Equal(t, "v2", string(res))
	assert.False(t, version.Ephemeral)
	assert.Equal(t, "", version.ClientIdentity)
	assert.NoError(t, client.Close())
}

func TestAsyncClientImpl_ClientIdentity(t *testing.T) {
	identity1 := newKey()
	client1, err := NewSyncClient(serviceAddress,
		WithIdentity(identity1),
	)
	assert.NoError(t, err)

	k := newKey()
	version, err := client1.Put(context.Background(), k, []byte("v1"), Ephemeral())
	assert.NoError(t, err)

	assert.True(t, version.Ephemeral)
	assert.Equal(t, identity1, version.ClientIdentity)

	client2, err := NewSyncClient(serviceAddress,
		WithSessionTimeout(2*time.Second),
	)
	assert.NoError(t, err)

	var res []byte
	res, version, err = client2.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version.ModificationsCount)
	assert.Equal(t, "v1", string(res))
	assert.True(t, version.Ephemeral)
	assert.Equal(t, identity1, version.ClientIdentity)

	version, err = client2.Put(context.Background(), k, []byte("v2"), Ephemeral())
	assert.NoError(t, err)

	assert.True(t, version.Ephemeral)
	assert.NotSame(t, "", version.ClientIdentity)

	assert.NoError(t, client1.Close())
	assert.NoError(t, client2.Close())
}
