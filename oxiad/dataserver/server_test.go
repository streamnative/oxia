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

package dataserver

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/common/security"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func TestNewServer(t *testing.T) {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.BindAddress = "localhost:0"
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()

	server, err := New(t.Context(), options)
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", server.metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))
}

func TestNewServerClosableWithHealthWatch(t *testing.T) {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.BindAddress = "localhost:0"
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()

	server, err := New(t.Context(), options)
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(nil, nil)
	defer clientPool.Close()

	client, err := clientPool.GetHealthRpc(fmt.Sprintf("127.0.0.1:%v", server.InternalPort()))
	assert.NoError(t, err)
	watchStream, err := client.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: ""})
	assert.NoError(t, err)

	err = server.Close()
	assert.NoError(t, err)

	assert.NoError(t, watchStream.CloseSend())
}

func TestNewServerAuthorityValidationFeatureFlag(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled by default", enabled: true},
		{name: "disabled by config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options := option.NewDefaultOptions()
			options.Server.Public.BindAddress = "localhost:0"
			options.Server.Internal.BindAddress = "localhost:0"
			options.Observability.Metric.Enabled = &constant.FlagFalse
			options.Storage.Database.Dir = t.TempDir()
			options.Storage.WAL.Dir = t.TempDir()
			if !tt.enabled {
				authorityValidation := false
				options.FeatureFlags.AuthorityValidation = &authorityValidation
			}

			server, err := New(t.Context(), options)
			if !assert.NoError(t, err) {
				return
			}
			defer server.Close()

			assert.Equal(t, tt.enabled, server.authorityValidationEnabled)
		})
	}
}

func TestNewServerRejectsSameWalAndDataDir(t *testing.T) {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	dir := filepath.Join(t.TempDir(), "data")
	options.Storage.Database.Dir = dir
	options.Storage.WAL.Dir = dir

	server, err := New(t.Context(), options)
	assert.ErrorContains(t, err, "are the same directory")
	assert.Nil(t, server)

	// Refused before writing anything, the manifest included
	_, err = os.Stat(dir)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestNewReleasesWhatItStartedOnError(t *testing.T) {
	// New fails to bind the public port, after it started the internal server
	publicListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer publicListener.Close()
	publicAddress := publicListener.Addr().String()

	// The internal port is picked up front, to check that New releases it
	internalListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	internalAddress := internalListener.Addr().String()
	require.NoError(t, internalListener.Close())

	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = publicAddress
	options.Server.Internal.BindAddress = internalAddress
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()

	server, err := New(t.Context(), options)
	require.ErrorContains(t, err, publicAddress)
	assert.Nil(t, server)

	// A gRPC server stopped before it started serving closes its listener in
	// its serve goroutine, which can run after New returned
	require.Eventually(t, func() bool {
		listener, err := net.Listen("tcp", internalAddress)
		if err != nil {
			return false
		}
		return listener.Close() == nil
	}, 10*time.Second, 10*time.Millisecond, "the internal port is still bound")

	// A retry binds the same internal address and opens the same directories
	options.Server.Public.BindAddress = "127.0.0.1:0"
	server, err = New(t.Context(), options)
	require.NoError(t, err)
	assert.NoError(t, server.Close())
}

func TestNewRejectsNilOptions(t *testing.T) {
	server, err := New(t.Context(), nil)
	assert.Error(t, err)
	assert.Nil(t, server)
}

func TestNewWithOptionsWatchRejectsSameWalAndDataDir(t *testing.T) {
	options := newTestServerOptions(t)
	options.Storage.WAL.Dir = options.Storage.Database.Dir

	server, err := NewWithOptionsWatch(t.Context(), commonwatch.New(options))
	assert.ErrorContains(t, err, "are the same directory")
	assert.Nil(t, server)
}

func TestNewWithOptionsWatchManifestError(t *testing.T) {
	options := newTestServerOptions(t)
	// The data dir can't be created under a file
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0600))
	options.Storage.Database.Dir = filepath.Join(file, "db")

	server, err := NewWithOptionsWatch(t.Context(), commonwatch.New(options))
	assert.Error(t, err)
	assert.Nil(t, server)
}

func TestNewWithOptionsWatchReplicationTLSError(t *testing.T) {
	options := newTestServerOptions(t)
	options.Replication.TLS = security.TLSOptions{
		Enabled:       &constant.FlagTrue,
		TrustedCaFile: filepath.Join(t.TempDir(), "missing-ca.pem"),
	}

	server, err := NewWithOptionsWatch(t.Context(), commonwatch.New(options))
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, server)
}

func TestNewWithGrpcProviderRejectsSameWalAndDataDir(t *testing.T) {
	options := newTestServerOptions(t)
	options.Storage.WAL.Dir = options.Storage.Database.Dir

	server, err := NewWithGrpcProvider(t.Context(), commonwatch.New(options), oxiadcommonrpc.Default,
		newNoOpReplicationRpcProvider(), newTestManifest(t, ""))
	assert.ErrorContains(t, err, "are the same directory")
	assert.Nil(t, server)
}

func TestNewWithGrpcProviderDatabaseError(t *testing.T) {
	options := newTestServerOptions(t)
	// The database can't be created under a file
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0600))
	options.Storage.Database.Dir = filepath.Join(file, "db")

	server, err := NewWithGrpcProvider(t.Context(), commonwatch.New(options), oxiadcommonrpc.Default,
		newNoOpReplicationRpcProvider(), newTestManifest(t, ""))
	assert.Error(t, err)
	assert.Nil(t, server)
}

func TestNewReleasesWhatItStartedOnStartupError(t *testing.T) {
	invalidTLS := security.TLSOptions{Enabled: &constant.FlagTrue}
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, options *option.Options)
	}{
		{name: "internal tls", setup: func(_ *testing.T, options *option.Options) {
			options.Server.Internal.TLS = invalidTLS
		}},
		{name: "internal bind", setup: func(t *testing.T, options *option.Options) {
			t.Helper()
			options.Server.Internal.BindAddress = boundAddress(t)
		}},
		{name: "public tls", setup: func(_ *testing.T, options *option.Options) {
			options.Server.Public.TLS = invalidTLS
		}},
		{name: "metric tls", setup: func(_ *testing.T, options *option.Options) {
			options.Observability.Metric.Enabled = &constant.FlagTrue
			options.Observability.Metric.BindAddress = "localhost:0"
			options.Observability.Metric.TLS = invalidTLS
		}},
		{name: "metric bind", setup: func(t *testing.T, options *option.Options) {
			t.Helper()
			options.Observability.Metric.Enabled = &constant.FlagTrue
			options.Observability.Metric.BindAddress = boundAddress(t)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options := newTestServerOptions(t)
			tt.setup(t, options)

			server, err := New(t.Context(), options)
			assert.Error(t, err)
			assert.Nil(t, server)

			// A retry opens the same directories
			retryOptions := newTestServerOptions(t)
			retryOptions.Storage = options.Storage
			server, err = New(t.Context(), retryOptions)
			require.NoError(t, err)
			assert.NoError(t, server.Close())
		})
	}
}

func TestServerReconfiguresLogLevel(t *testing.T) {
	options := newTestServerOptions(t)
	options.WithDefault()
	optionsWatch := commonwatch.New(options)
	server, err := NewWithOptionsWatch(t.Context(), optionsWatch)
	require.NoError(t, err)
	defer server.Close()

	logEnabled := func(level slog.Level) bool {
		return slog.Default().Enabled(context.Background(), level)
	}
	require.False(t, logEnabled(slog.LevelDebug))
	publishLogLevel := func(level string) {
		updated := *options
		updated.Observability.Log.Level = level
		optionsWatch.Publish(&updated)
	}

	publishLogLevel("debug")
	assert.Eventually(t, func() bool { return logEnabled(slog.LevelDebug) }, 10*time.Second, 10*time.Millisecond)

	publishLogLevel("info")
	assert.Eventually(t, func() bool { return !logEnabled(slog.LevelDebug) }, 10*time.Second, 10*time.Millisecond)
}

func newTestServerOptions(t *testing.T) *option.Options {
	t.Helper()
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()
	return options
}

// boundAddress returns an address that a listener holds until the end of the test.
func boundAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String()
}
