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

package kv

import (
	"context"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/proto"
	"testing"
	"time"
)

func init() {
	common.ConfigureLogger()
}

func TestDB_Notifications(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(1, factory)
	assert.NoError(t, err)

	t0 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("0"),
		}},
	}, 0, t0, NoOpCallback)

	notifications, err := db.ReadNextNotifications(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb := notifications[0]
	assert.Equal(t, t0, nb.Timestamp)
	assert.EqualValues(t, 0, nb.Offset)
	assert.EqualValues(t, 1, nb.ShardId)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found := nb.Notifications["a"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyCreated, n.Type)
	assert.EqualValues(t, 0, *n.Version)

	t1 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("1"),
		}},
	}, 1, t1, NoOpCallback)

	t2 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "b",
			Payload: []byte("0"),
		}},
	}, 2, t2, NoOpCallback)

	notifications, err = db.ReadNextNotifications(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(notifications))

	nb = notifications[0]
	assert.Equal(t, t1, nb.Timestamp)
	assert.EqualValues(t, 1, nb.Offset)
	assert.EqualValues(t, 1, nb.ShardId)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = nb.Notifications["a"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyModified, n.Type)
	assert.EqualValues(t, 1, *n.Version)

	nb = notifications[1]
	assert.Equal(t, t2, nb.Timestamp)
	assert.EqualValues(t, 2, nb.Offset)
	assert.EqualValues(t, 1, nb.ShardId)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = nb.Notifications["b"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyCreated, n.Type)
	assert.EqualValues(t, 0, *n.Version)

	/// Write one batch
	t3 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "c",
			Payload: []byte("0"),
		}, {
			Key:     "d",
			Payload: []byte("0"),
		}},
		Deletes: []*proto.DeleteRequest{{
			Key: "a",
		}},
	}, 3, t3, NoOpCallback)

	notifications, err = db.ReadNextNotifications(context.Background(), 3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb = notifications[0]
	assert.Equal(t, t3, nb.Timestamp)
	assert.EqualValues(t, 3, nb.Offset)
	assert.EqualValues(t, 1, nb.ShardId)
	assert.Equal(t, 3, len(nb.Notifications))
	n, found = nb.Notifications["c"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyCreated, n.Type)
	assert.EqualValues(t, 0, *n.Version)
	n, found = nb.Notifications["d"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyCreated, n.Type)
	assert.EqualValues(t, 0, *n.Version)
	n, found = nb.Notifications["a"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyDeleted, n.Type)
	assert.Nil(t, n.Version)

	// When there are multiple keys in one batch, only 1 notification
	// is going to get triggered
	t4 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "x1",
			Payload: []byte("0"),
		}, {
			Key:     "x1",
			Payload: []byte("1"),
		}},
	}, 4, t4, NoOpCallback)

	notifications, err = db.ReadNextNotifications(context.Background(), 4)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb = notifications[0]
	assert.Equal(t, t4, nb.Timestamp)
	assert.EqualValues(t, 4, nb.Offset)
	assert.EqualValues(t, 1, nb.ShardId)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = nb.Notifications["x1"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KeyModified, n.Type)
	assert.EqualValues(t, 1, *n.Version)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_NotificationsCancelWait(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(1, factory)
	assert.NoError(t, err)

	t0 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("0"),
		}},
	}, 0, t0, NoOpCallback)

	ctx, cancel := context.WithCancel(context.Background())

	doneCh := make(chan error)

	go func() {
		notifications, err := db.ReadNextNotifications(ctx, 5)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, notifications)
		close(doneCh)
	}()

	// Cancel the context to trigger exit from the wait
	cancel()

	select {
	case <-doneCh:
	// Ok

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Should not have timed out")
	}

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}
