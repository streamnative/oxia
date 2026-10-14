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
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver/dns"
)

func TestMain(m *testing.M) {
	// Same as cmd/main.go
	dns.SetMinResolutionInterval(time.Second)
	m.Run()
}

func TestConnectionReconnectsToNewAddressOfRestartedServer(t *testing.T) {
	oldListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(oldListener.Addr().String())
	require.NoError(t, err)
	newListener, err := net.Listen("tcp", net.JoinHostPort("::1", port))
	if err != nil {
		require.NoError(t, oldListener.Close())
		t.Skipf("cannot listen on [::1]:%s: %v", port, err)
	}
	oldServer := serveTestHealth(t, oldListener)

	dnsServer := newTestDNSServer(t, "127.0.0.1")
	target := "dns://" + dnsServer.addr + "/" + net.JoinHostPort("oxia.test", port)
	conn, err := newConnectionWithHealthConfig(target, nil, nil, nil, func(string) {}, time.Hour, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.Eventually(t, func() bool { return checkTestConnection(conn) == nil }, 5*time.Second, 10*time.Millisecond)

	// Restart the server on a new IP, after the connection resolved the old one
	dnsServer.host.Store("::1")
	oldServer.Stop()
	serveTestHealth(t, newListener)

	// With the default resolver options, the name is not looked up again for 30s
	require.Eventually(t, func() bool { return checkTestConnection(conn) == nil }, 10*time.Second, 50*time.Millisecond)
}

func checkTestConnection(conn *connection) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := conn.healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}

// testDNSServer answers the A and AAAA queries of any name with host.
type testDNSServer struct {
	addr string
	host atomic.Value
}

func newTestDNSServer(t *testing.T, host string) *testDNSServer {
	t.Helper()

	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = packetConn.Close() })

	s := &testDNSServer{addr: packetConn.LocalAddr().String()}
	s.host.Store(host)
	go func() {
		buf := make([]byte, 512)
		for {
			n, from, err := packetConn.ReadFrom(buf)
			if err != nil {
				return
			}
			if response, err := s.answer(buf[:n]); err == nil {
				_, _ = packetConn.WriteTo(response, from)
			}
		}
	}()
	return s
}

func (s *testDNSServer) answer(query []byte) ([]byte, error) {
	var request dnsmessage.Message
	if err := request.Unpack(query); err != nil {
		return nil, err
	}
	response := dnsmessage.Message{
		Header:    dnsmessage.Header{ID: request.ID, Response: true, Authoritative: true},
		Questions: request.Questions,
	}
	addr := netip.MustParseAddr(s.host.Load().(string))
	for _, q := range request.Questions {
		header := dnsmessage.ResourceHeader{Name: q.Name, Type: q.Type, Class: q.Class, TTL: 0}
		switch {
		case q.Type == dnsmessage.TypeA && addr.Is4():
			response.Answers = append(response.Answers,
				dnsmessage.Resource{Header: header, Body: &dnsmessage.AResource{A: addr.As4()}})
		case q.Type == dnsmessage.TypeAAAA && addr.Is6():
			response.Answers = append(response.Answers,
				dnsmessage.Resource{Header: header, Body: &dnsmessage.AAAAResource{AAAA: addr.As16()}})
		default:
			// No record of this type: answer with an empty section
		}
	}
	return response.Pack()
}

func serveTestHealth(t *testing.T, listener net.Listener) *grpc.Server {
	t.Helper()

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)
	return server
}
