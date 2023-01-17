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
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/server"
	"testing"
	"time"
)

func init() {
	common.ConfigureLogger()
}

func TestAsyncClientImpl(t *testing.T) {
	server, err := server.NewStandalone(server.NewTestConfig())
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.RpcPort())
	client, err := NewAsyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	putResult := <-client.Put("/a", []byte{0}, ExpectedVersion(VersionNotExists))
	assert.EqualValues(t, 0, putResult.Stat.Version)

	getResult := <-client.Get("/a")
	assert.Equal(t, GetResult{
		Payload: []byte{0},
		Stat:    putResult.Stat,
	}, getResult)

	putResult = <-client.Put("/c", []byte{0}, ExpectedVersion(VersionNotExists))
	assert.EqualValues(t, 0, putResult.Stat.Version)

	putResult = <-client.Put("/c", []byte{1}, ExpectedVersion(0))
	assert.EqualValues(t, 1, putResult.Stat.Version)

	getRangeResult := <-client.List("/a", "/d")
	assert.Equal(t, ListResult{
		Keys: []string{"/a", "/c"},
	}, getRangeResult)

	deleteErr := <-client.Delete("/a", ExpectedVersion(0))
	assert.NoError(t, deleteErr)

	getResult = <-client.Get("/a")
	assert.Equal(t, GetResult{
		Err: ErrorKeyNotFound,
	}, getResult)

	deleteRangeResult := <-client.DeleteRange("/c", "/d")
	assert.NoError(t, deleteRangeResult)

	getResult = <-client.Get("/d")
	assert.Equal(t, GetResult{
		Err: ErrorKeyNotFound,
	}, getResult)

	err = client.Close()
	assert.NoError(t, err)

	err = server.Close()
	assert.NoError(t, err)
}

func TestAsyncClientImpl_Notifications(t *testing.T) {
	server, err := server.NewStandalone(server.NewTestConfig())
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.RpcPort())
	client, err := NewSyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	notifications, err := client.GetNotifications()
	assert.NoError(t, err)

	s1, _ := client.Put("/a", []byte("0"))

	n := <-notifications.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s1.Version, n.Version)

	s2, _ := client.Put("/a", []byte("1"))

	n = <-notifications.Ch()
	assert.Equal(t, KeyModified, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s2.Version, n.Version)

	s3, _ := client.Put("/b", []byte("0"))
	assert.NoError(t, client.Delete("/a"))

	n = <-notifications.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/b", n.Key)
	assert.Equal(t, s3.Version, n.Version)

	n = <-notifications.Ch()
	assert.Equal(t, KeyDeleted, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.EqualValues(t, -1, n.Version)

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

	s4, _ := client.Put("/x", []byte("1"))

	n = <-notifications.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.Version, n.Version)

	n = <-notifications2.Ch()
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.Version, n.Version)

	////

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

	assert.NoError(t, server.Close())
}

func TestAsyncClientImpl_NotificationsClose(t *testing.T) {
	server, err := server.NewStandalone(server.NewTestConfig())
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.RpcPort())
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
	assert.NoError(t, server.Close())
}

func TestAsyncClientImpl_Sessions(t *testing.T) {
	server, err := server.NewStandalone(server.NewTestConfig())
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.RpcPort())
	client, err := NewAsyncClient(serviceAddress, WithBatchLinger(0), WithSessionTimeout(5*time.Second))
	assert.NoError(t, err)

	putCh := client.Put("/x", []byte("x"), Ephemeral)
	select {
	case res := <-putCh:
		assert.NotNil(t, res)
		assert.NoError(t, res.Err)
		assert.EqualValues(t, 0, res.Stat.Version)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}
	getCh := client.Get("/x")
	select {
	case res := <-getCh:
		assert.NotNil(t, res)
		assert.NoError(t, res.Err)
		assert.EqualValues(t, 0, res.Stat.Version)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}
	assert.NoError(t, client.Close())
	log.Debug().Msg("First client closed")

	client, err = NewAsyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		getCh = client.Get("/x")
		select {
		case res := <-getCh:
			assert.NotNil(t, res)
			log.Debug().Interface("res", res).Msg("Get resulted in")
			return errors.Is(res.Err, ErrorKeyNotFound)

		case <-time.After(1 * time.Second):
			assert.Fail(t, "Shouldn't have timed out")
			return false
		}

	}, 8*time.Second, 500*time.Millisecond)

	assert.NoError(t, client.Close())
	assert.NoError(t, server.Close())

}
