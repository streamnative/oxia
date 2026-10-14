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

package rpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestConnectionReconnectsToNewAddressOfRestartedServer(t *testing.T) {
	oldServer, _, oldAddress := newTestHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)

	// Stands in for DNS: the dialer resolves the name of the target on every dial
	var address atomic.Value
	address.Store(oldAddress)
	dialer := grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
		assert.Equal(t, "oxia.test:6649", target)
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address.Load().(string))
	})

	conn, err := newConnectionWithHealthConfig("oxia.test:6649", nil, nil, []grpc.DialOption{dialer},
		func(string) {}, time.Hour, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.Eventually(t, func() bool { return checkTestConnection(conn) == nil }, 5*time.Second, 10*time.Millisecond)

	// Restart the server on a new address
	oldServer.Stop()
	_, _, newAddress := newTestHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)
	address.Store(newAddress)

	require.Eventually(t, func() bool { return checkTestConnection(conn) == nil }, 5*time.Second, 50*time.Millisecond)
}

func checkTestConnection(conn *connection) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := conn.healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}
