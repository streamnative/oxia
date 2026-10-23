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

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestServerOptions(t *testing.T) {
	so := newServerOptions(nil)
	assert.Nil(t, so.onLeadershipLost, "no handler selects the default")
	assert.Nil(t, so.initialClusterConfig)
	require.NoError(t, so.validate())

	config := &proto.ClusterConfiguration{}
	called := false
	so = newServerOptions([]ServerOption{
		WithOnLeadershipLost(func() { called = true }),
		WithInitialClusterConfiguration(config),
	})

	so.onLeadershipLost()
	assert.True(t, called)
	assert.Same(t, config, so.initialClusterConfig)
	require.NoError(t, so.validate())
}

// A nil option is ignored and a nil leadership-loss handler keeps the default:
// neither can leave the coordinator with a nil to call.
func TestServerOptionsNilSafe(t *testing.T) {
	so := newServerOptions([]ServerOption{nil, WithOnLeadershipLost(nil)})
	assert.Nil(t, so.onLeadershipLost, "a nil handler selects the default")
	assert.Nil(t, so.initialClusterConfig)
}

func TestServerOptionsRejectInvalidInitialClusterConfiguration(t *testing.T) {
	so := newServerOptions([]ServerOption{
		WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: "default", ReplicationFactor: 1}},
		}),
	})
	require.ErrorContains(t, so.validate(), "initialShardCount")
}

func TestNewRejectsNilOptions(t *testing.T) {
	_, err := New(t.Context(), nil)
	require.ErrorContains(t, err, "options must not be nil")
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	options := newTestOptions()
	options.Metadata.ProviderName = "etcd"

	_, err := New(t.Context(), options)
	require.ErrorContains(t, err, "must be one of")
}

// NewGrpcServer doesn't validate the options, which a watch supplies: each
// part that fails to start fails it, and releases what it started before.
func TestNewGrpcServerFailsToStart(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, occupied.Close()) })
	missingFile := filepath.Join(t.TempDir(), "missing.pem")
	invalidTLS := security.TLSOptions{
		Enabled:       &constant.FlagTrue,
		CertFile:      missingFile,
		KeyFile:       missingFile,
		TrustedCaFile: missingFile,
	}
	notADirectory := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADirectory, nil, 0o600))

	for _, test := range []struct {
		name   string
		modify func(options *option.Options)
	}{
		{"internal TLS", func(options *option.Options) { options.Server.Internal.TLS = invalidTLS }},
		{"internal listener", func(options *option.Options) {
			options.Server.Internal.BindAddress = occupied.Addr().String()
		}},
		{"controller TLS", func(options *option.Options) { options.Controller.TLS = invalidTLS }},
		{"metadata", func(options *option.Options) {
			options.Metadata.ProviderName = option.ProviderFile
			options.Metadata.File.Dir = filepath.Join(notADirectory, "metadata")
		}},
		{"public TLS", func(options *option.Options) { options.Server.Public.TLS = invalidTLS }},
		{"public listener", func(options *option.Options) {
			options.Server.Public.BindAddress = occupied.Addr().String()
		}},
		{"leadership", func(options *option.Options) {
			// The file provider locks the status file, which it creates
			readOnly := filepath.Join(t.TempDir(), "read-only")
			require.NoError(t, os.Mkdir(readOnly, 0o500))
			options.Metadata.ProviderName = option.ProviderFile
			options.Metadata.File.StatusName = filepath.Join(readOnly, option.DefaultFileStatusName)
			options.Metadata.File.ConfigName = filepath.Join(t.TempDir(), option.DefaultFileConfigName)
		}},
		{"metrics TLS", func(options *option.Options) {
			options.Observability.Metric = commonoption.MetricOptions{
				Enabled:     &constant.FlagTrue,
				BindAddress: "localhost:0",
				TLS:         invalidTLS,
			}
		}},
		{"metrics listener", func(options *option.Options) {
			options.Observability.Metric = commonoption.MetricOptions{
				Enabled:     &constant.FlagTrue,
				BindAddress: occupied.Addr().String(),
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := newTestOptions()
			test.modify(options)

			server, err := NewGrpcServer(t.Context(), commonwatch.New(options))
			require.Error(t, err)
			assert.Nil(t, server)
		})
	}
}

// A configuration to seed that can't be stored fails the start, after the
// coordinator became the leader.
func TestNewGrpcServerFailsToSeedClusterConfiguration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	readOnly := filepath.Join(t.TempDir(), "read-only")
	require.NoError(t, os.Mkdir(readOnly, 0o500))
	options := newTestOptions()
	options.Metadata.ProviderName = option.ProviderFile
	options.Metadata.File.StatusName = filepath.Join(t.TempDir(), option.DefaultFileStatusName)
	options.Metadata.File.ConfigName = filepath.Join(readOnly, option.DefaultFileConfigName)

	server, err := NewGrpcServer(t.Context(), commonwatch.New(options),
		WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: "default", ReplicationFactor: 1, InitialShardCount: 1}},
			Servers:    []*proto.DataServerIdentity{{Public: "s1:6648", Internal: "s1:6649"}},
		}))
	require.ErrorIs(t, err, os.ErrPermission)
	assert.Nil(t, server)
}

// A runtime that fails to start fails the start, after the coordinator became
// the leader.
func TestNewGrpcServerFailsToStartRuntime(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	failpointErr := errors.New("failpoint")
	options := newTestOptions()
	failpoints := failpoint.New(map[string]func() error{
		"newGrpcServer.newRuntime": func() error { return failpointErr },
	})

	server, err := newGrpcServer(t.Context(), commonwatch.New(options), failpoints)
	require.ErrorIs(t, err, failpointErr)
	assert.Nil(t, server)
}

// A cluster configuration that can't be reconciled fails the start, after the
// coordinator became the leader.
func TestNewGrpcServerFailsToReconcileClusterConfiguration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	statusPath := filepath.Join(t.TempDir(), option.DefaultFileStatusName)
	require.NoError(t, os.WriteFile(statusPath, []byte("instanceId: coordinator-test\n"), 0o400))
	options := newTestOptions()
	options.Metadata.ProviderName = option.ProviderFile
	options.Metadata.File.StatusName = statusPath
	options.Metadata.File.ConfigName = filepath.Join(t.TempDir(), option.DefaultFileConfigName)

	server, err := NewGrpcServer(t.Context(), commonwatch.New(options),
		WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: "default", ReplicationFactor: 1, InitialShardCount: 1}},
			Servers:    []*proto.DataServerIdentity{{Public: "s1:6648", Internal: "s1:6649"}},
		}))
	require.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorContains(t, err, "failed to reconcile the cluster configuration")
	assert.Nil(t, server)
}

func TestGrpcServerAppliesLogLevelChanges(t *testing.T) {
	logging.ConfigureLogger()
	originalLevel := logging.LogLevel
	options := newTestOptions()
	options.Observability.Log.Level = originalLevel.String()
	optionsWatch := commonwatch.New(options)
	server, err := NewGrpcServer(t.Context(), optionsWatch)
	require.NoError(t, err)
	assert.NotZero(t, server.InternalPort())
	assert.NotZero(t, server.PublicPort())

	updated := newTestOptions()
	updated.Observability.Log.Level = slog.LevelDebug.String()
	if originalLevel == slog.LevelDebug {
		updated.Observability.Log.Level = slog.LevelInfo.String()
	}
	expectedLevel, err := logging.ParseLogLevel(updated.Observability.Log.Level)
	require.NoError(t, err)
	// The server subscribes to the changes in the background: publish until it
	// sees one
	require.Eventually(t, func() bool {
		optionsWatch.Publish(updated)
		return slog.Default().Enabled(context.Background(), expectedLevel) &&
			!slog.Default().Enabled(context.Background(), expectedLevel-1)
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, server.Close())
	logging.ReconfigureLogger(&commonoption.LogOptions{Level: originalLevel.String()})
}

func TestGrpcServerWatchLeadershipEndsOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	server := &GrpcServer{ctx: ctx, logger: slog.Default()}
	called := false

	server.watchLeadership(make(chan struct{}), func() { called = true })
	assert.False(t, called)
}

// Without a handler, a leadership loss terminates the process.
func TestGrpcServerWatchLeadershipTerminatesWithoutHandler(t *testing.T) {
	terminated := false
	original := onLeadershipLost
	onLeadershipLost = func() { terminated = true }
	t.Cleanup(func() { onLeadershipLost = original })
	lost := make(chan struct{})
	close(lost)
	server := &GrpcServer{ctx: t.Context(), logger: slog.Default()}

	server.watchLeadership(lost, nil)
	assert.True(t, terminated)
}

// A coordinator that fails to stop part of its coordination still stops the
// rest before the handler is called.
func TestGrpcServerWatchLeadershipStopsCoordinatingDespiteCloseFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	factory, err := coordmetadata.New(ctx, newTestOptions())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	metadata := factory.CreateMetadata(ctx)
	closeErr := errors.New("close failure")
	server := &GrpcServer{
		ctx:          ctx,
		ctxCancel:    cancel,
		logger:       slog.Default(),
		healthServer: health.NewServer(),
		management:   newManagementServer(metadata),
		reconciler:   &fakeReconciler{closeErr: closeErr},
		runtime:      &testRuntime{},
		metadata:     metadata,
		leader:       &atomic.Bool{},
	}
	lost := make(chan struct{})
	close(lost)
	called := false

	server.watchLeadership(lost, func() { called = true })
	assert.True(t, called)
	require.ErrorIs(t, server.stopCoordinating(), closeErr)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	checked, err := server.healthServer.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, checked.GetStatus())
}

// A standby coordinator serves its metrics while it waits for the leadership,
// and reports whether it holds it.
func TestGrpcServerServesMetricsAsStandby(t *testing.T) {
	options := newTestOptions()
	options.Metadata.ProviderName = option.ProviderFile
	options.Metadata.File.StatusName = filepath.Join(t.TempDir(), option.DefaultFileStatusName)
	options.Metadata.File.ConfigName = filepath.Join(t.TempDir(), option.DefaultFileConfigName)
	holderFactory, err := coordmetadata.New(t.Context(), options)
	require.NoError(t, err)
	holder := holderFactory.CreateMetadata(t.Context())
	_, err = holder.WaitToBecomeLeader()
	require.NoError(t, err)

	free, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	metricsAddress := free.Addr().String()
	require.NoError(t, free.Close())
	options.Observability.Metric = commonoption.MetricOptions{
		Enabled:     &constant.FlagTrue,
		BindAddress: metricsAddress,
	}
	type startResult struct {
		server *GrpcServer
		err    error
	}
	started := make(chan startResult, 1)
	go func() {
		server, err := NewGrpcServer(t.Context(), commonwatch.New(options))
		started <- startResult{server: server, err: err}
	}()
	requireLeader := func(expected int) {
		t.Helper()
		pattern := regexp.MustCompile(fmt.Sprintf(`(?m)^oxia_coordinator_is_leader(\{[^}]*\})? %d$`, expected))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+metricsAddress+"/metrics", http.NoBody)
			require.NoError(c, err)
			response, err := http.DefaultClient.Do(request)
			require.NoError(c, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(c, err)
			assert.Regexp(c, pattern, string(body))
		}, 10*time.Second, 10*time.Millisecond)
	}

	requireLeader(0)
	select {
	case result := <-started:
		require.FailNow(t, "the standby started while the leadership is held", "err: %v", result.err)
	default:
	}

	require.NoError(t, holder.Close())
	require.NoError(t, holderFactory.Close())
	var result startResult
	select {
	case result = <-started:
		require.NoError(t, result.err)
	case <-time.After(time.Minute):
		require.FailNow(t, "the standby did not become the leader")
	}
	requireLeader(1)
	require.NoError(t, result.server.Close())
}

// newTestOptions returns the options of a coordinator on the memory metadata,
// listening on free ports, without metrics.
func newTestOptions() *option.Options {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = "coordinator-test"
	options.Metadata.ProviderName = option.ProviderMemory
	return options
}

type fakeReconciler struct {
	closeErr error
}

func (r *fakeReconciler) Close() error {
	return r.closeErr
}

func (*fakeReconciler) Reconcile(context.Context, *proto.ClusterConfiguration) error {
	return nil
}
