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

package notifications

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func TestExecReturnsNewClientError(t *testing.T) {
	mocked, auth := common.MockedClient, common.Config.Auth
	common.MockedClient = nil
	common.Config.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { common.MockedClient, common.Config.Auth = mocked, auth })

	err := runCmd()

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func TestExecReturnsNotificationsErrorAndClosesClient(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
	common.MockedClient.On("GetNotifications").Return(nil, errNotifications)
	common.MockedClient.On("Close").Return(nil)

	err := runCmd()

	assert.ErrorIs(t, err, errNotifications)
	common.MockedClient.AssertExpectations(t)
}

func TestExecReturnsWhenNotificationsEnd(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
	notifications := &fakeNotifications{ch: make(chan *oxia.Notification)}
	close(notifications.ch)
	common.MockedClient.On("GetNotifications").Return(notifications, nil)
	common.MockedClient.On("Close").Return(nil)

	require.NoError(t, runCmd())

	assert.True(t, notifications.closed)
	common.MockedClient.AssertExpectations(t)
}

func TestExecLogsNotifications(t *testing.T) {
	standalone, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, standalone.Close()) })
	client, err := oxia.NewSyncClient(standalone.ServiceAddr())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	handler := newRecordingHandler()
	defaultLogger, config := slog.Default(), common.Config
	slog.SetDefault(slog.New(handler))
	common.Config = common.ClientConfig{
		ServiceAddr:    standalone.ServiceAddr(),
		Namespace:      oxia.DefaultNamespace,
		RequestTimeout: oxia.DefaultRequestTimeout,
	}
	t.Cleanup(func() { slog.SetDefault(defaultLogger) })
	t.Cleanup(func() { common.Config = config })

	ctx, cancel := context.WithCancel(t.Context())
	Cmd.SetArgs(nil)
	go func() {
		handler.errs <- Cmd.ExecuteContext(ctx)
	}()

	var created map[string]any
	for i := 0; created == nil && i < 100; i++ {
		_, _, err := client.Put(ctx, fmt.Sprintf("/key-%d", i), []byte("value"))
		require.NoError(t, err)
		created, err = handler.waitFor(oxia.KeyCreated.String(), 100*time.Millisecond)
		if !errors.Is(err, errWaitTimeout) {
			require.NoError(t, err)
		}
	}
	require.NotNil(t, created)
	assert.Contains(t, created, "version-id")

	_, _, err = client.Put(ctx, created["key"].(string), []byte("value-2"))
	require.NoError(t, err)
	modified, err := handler.waitFor(oxia.KeyModified.String(), 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, created["key"], modified["key"])
	assert.Contains(t, modified, "version-id")

	require.NoError(t, client.DeleteRange(ctx, "/key-", "/key-~"))
	deleted, err := handler.waitFor(oxia.KeyRangeRangeDeleted.String(), 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "/key-", deleted["key"])
	assert.Equal(t, "/key-~", deleted["key-range-end"])

	cancel()
	assert.NoError(t, <-handler.errs)
}

func runCmd() error {
	out := new(bytes.Buffer)
	Cmd.SetOut(out)
	Cmd.SetErr(out)
	Cmd.SetArgs(nil)
	return Cmd.Execute()
}

var (
	errWaitTimeout   = errors.New("timed out waiting for the notification")
	errNotifications = errors.New("notifications failed")
)

type fakeNotifications struct {
	ch     chan *oxia.Notification
	closed bool
}

func (n *fakeNotifications) Ch() <-chan *oxia.Notification {
	return n.ch
}

func (n *fakeNotifications) Close() error {
	n.closed = true
	return nil
}

type recordingHandler struct {
	records chan map[string]any
	errs    chan error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{records: make(chan map[string]any, 100), errs: make(chan error, 1)}
}

func (*recordingHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "" {
		return nil
	}
	attrs := map[string]any{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	select {
	case h.records <- attrs:
	default:
	}
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordingHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordingHandler) waitFor(notificationType string, timeout time.Duration) (map[string]any, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case attrs := <-h.records:
			if attrs["type"] == notificationType {
				return attrs, nil
			}
		case err := <-h.errs:
			return nil, err
		case <-timer.C:
			return nil, errWaitTimeout
		}
	}
}
