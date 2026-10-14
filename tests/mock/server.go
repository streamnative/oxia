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

package mock

import (
	"fmt"
	"math/rand/v2"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func NewServerWithOptions(t *testing.T, name string, optionsConsumer func(options *option.Options)) (s *dataserver.Server, addr *commonproto.DataServerIdentity) {
	t.Helper()
	dataServerOption := option.NewDefaultOptions()
	dataServerOption.Server.Public.BindAddress = "localhost:0"
	dataServerOption.Server.Internal.BindAddress = "localhost:0"
	dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
	dataServerOption.Storage.Database.Dir = t.TempDir()
	dataServerOption.Storage.WAL.Dir = t.TempDir()
	optionsConsumer(dataServerOption)
	var err error
	s, err = dataserver.New(t.Context(), dataServerOption)

	assert.NoError(t, err)

	tmp := &name
	addr = &commonproto.DataServerIdentity{
		Name:     tmp,
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func NewServer(t *testing.T, name string) (s *dataserver.Server, addr *commonproto.DataServerIdentity) {
	t.Helper()
	return NewServerWithOptions(t, name, func(_ *option.Options) {
	})
}

// FreeAddresses picks the ports in [minFixedPort, ephemeralPortsStart): above
// the fixed ports that other tests bind, e.g. Oxia's default 6648, and below
// the default ephemeral port ranges of Linux (32768-60999), macOS and Windows
// (49152-65535).
const (
	minFixedPort        = 10000
	ephemeralPortsStart = 32768
)

// FreeAddresses returns n loopback addresses with distinct ports that were
// free a moment ago, for the servers that must bind an address known before
// they start, e.g. a data server that the cluster configuration lists while
// it is down.
//
// The kernel doesn't give these ports to another socket while they are free,
// e.g. to a listener of a test package that runs in parallel, so only an
// explicit bind can take one. The scan starts at a random port, so that the
// test packages that run in parallel rarely pick the same one.
func FreeAddresses(t *testing.T, n int) []string {
	t.Helper()

	const ports = ephemeralPortsStart - minFixedPort
	addresses := make([]string, 0, n)
	listenConfig := net.ListenConfig{}
	start := rand.IntN(ports) //nolint:gosec
	for i := 0; len(addresses) < n; i++ {
		require.Less(t, i, ports, "not enough free ports below the ephemeral range")
		port := minFixedPort + (start+i)%ports
		listener, err := listenConfig.Listen(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		addresses = append(addresses, listener.Addr().String())
		require.NoError(t, listener.Close())
	}
	return addresses
}

func NewServerWithAddress(t *testing.T, name string, publicAddress string, internalAddress string) (s *dataserver.Server, addr *commonproto.DataServerIdentity) {
	t.Helper()
	dataServerOption := option.NewDefaultOptions()
	dataServerOption.Server.Public.BindAddress = publicAddress
	dataServerOption.Server.Internal.BindAddress = internalAddress
	dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
	dataServerOption.Storage.Database.Dir = t.TempDir()
	dataServerOption.Storage.WAL.Dir = t.TempDir()

	var err error
	s, err = dataserver.New(t.Context(), dataServerOption)

	require.NoError(t, err)

	tmp := &name
	addr = &commonproto.DataServerIdentity{
		Name:     tmp,
		Public:   publicAddress,
		Internal: internalAddress,
	}

	return s, addr
}
