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

package sequenceupdates

import (
	"bytes"
	"context"
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

func TestFlagsReset(t *testing.T) {
	config := flags{key: "key", partitionKey: "partition-key"}

	config.Reset()

	assert.Equal(t, flags{}, config)
}

func TestExecReturnsNewClientError(t *testing.T) {
	mocked, auth := common.MockedClient, common.Config.Auth
	common.MockedClient = nil
	common.Config.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { common.MockedClient, common.Config.Auth = mocked, auth })

	err := runCmd("key", "-p", "partition-key")

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func TestExecReturnsSequenceUpdatesErrorAndClosesClient(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
	common.MockedClient.On("Close").Return(nil)

	err := runCmd("key", "-p", "partition-key")

	assert.EqualError(t, err, "not implemented in mock")
	common.MockedClient.AssertExpectations(t)
}

func TestExecLogsSequenceUpdates(t *testing.T) {
	standalone, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, standalone.Close()) })
	client, err := oxia.NewSyncClient(standalone.ServiceAddr())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	handler := newRecordingHandler()
	defaultLogger, clientConfig := slog.Default(), common.Config
	slog.SetDefault(slog.New(handler))
	common.Config = common.ClientConfig{
		ServiceAddr:    standalone.ServiceAddr(),
		Namespace:      oxia.DefaultNamespace,
		RequestTimeout: oxia.DefaultRequestTimeout,
	}
	t.Cleanup(func() { slog.SetDefault(defaultLogger) })
	t.Cleanup(func() { common.Config = clientConfig })
	t.Cleanup(Config.Reset)

	ctx, cancel := context.WithCancel(t.Context())
	Cmd.SetArgs([]string{"/seq", "--partition-key", "partition-key"})
	go func() {
		handler.errs <- Cmd.ExecuteContext(ctx)
	}()

	var key string
	for i := 0; key == "" && i < 100; i++ {
		_, _, err := client.Put(context.Background(), "/seq", []byte("value"),
			oxia.PartitionKey("partition-key"), oxia.SequenceKeysDeltas(1))
		require.NoError(t, err)
		key, err = handler.waitForKey(100 * time.Millisecond)
		require.NoError(t, err)
	}
	assert.Contains(t, key, "/seq-")

	cancel()
	assert.NoError(t, <-handler.errs)
}

func runCmd(args ...string) error {
	out := new(bytes.Buffer)
	Cmd.SetOut(out)
	Cmd.SetErr(out)
	Cmd.SetArgs(args)
	err := Cmd.Execute()
	Config.Reset()
	return err
}

type recordingHandler struct {
	keys chan string
	errs chan error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{keys: make(chan string, 100), errs: make(chan error, 1)}
}

func (*recordingHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "" {
		return nil
	}
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "key" {
			select {
			case h.keys <- attr.Value.String():
			default:
			}
		}
		return true
	})
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordingHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordingHandler) waitForKey(timeout time.Duration) (string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case key := <-h.keys:
		return key, nil
	case err := <-h.errs:
		return "", err
	case <-timer.C:
		return "", nil
	}
}
