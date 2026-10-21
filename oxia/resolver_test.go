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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
)

func TestGrpcResolverResolveNowLeavesUpdatesToServiceResolver(t *testing.T) {
	serviceResolver := &testServiceResolver{addresses: []string{"host-1:6648", "host-2:6648"}}
	clientConn := &testResolverClientConn{}
	builder := &grpcResolverBuilder{sr: serviceResolver}

	r, err := builder.Build(resolver.Target{URL: url.URL{Scheme: "test", Path: "/service"}}, clientConn,
		resolver.BuildOptions{})
	require.NoError(t, err)
	r.ResolveNow(resolver.ResolveNowOptions{})
	r.Close()

	assert.Equal(t, []string{"service"}, serviceResolver.endpoints)
	require.Len(t, clientConn.states, 1)
	assert.Equal(t, []resolver.Address{{Addr: "host-1:6648"}, {Addr: "host-2:6648"}},
		clientConn.states[0].Addresses)
}

// testServiceResolver records the endpoints it resolves, and resolves each one
// to addresses.
type testServiceResolver struct {
	addresses []string
	endpoints []string
}

func (*testServiceResolver) Scheme() string { return "test" }

func (r *testServiceResolver) Resolve(endpoint string, updater AddressUpdater) {
	r.endpoints = append(r.endpoints, endpoint)
	_ = updater(r.addresses)
}

// testResolverClientConn records the states it is updated with.
type testResolverClientConn struct {
	resolver.ClientConn
	states []resolver.State
}

func (c *testResolverClientConn) UpdateState(state resolver.State) error {
	c.states = append(c.states, state)
	return nil
}
