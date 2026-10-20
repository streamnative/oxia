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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/security"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

// A target that gRPC can't parse, to fail creating the connection.
const invalidTarget = "%zz://invalid"

func TestNewReplicationRpcProviderInvalidTLS(t *testing.T) {
	enabled := true
	tlsOptions := &security.TLSOptions{
		Enabled:       &enabled,
		TrustedCaFile: filepath.Join(t.TempDir(), "missing-ca.pem"),
	}

	_, err := NewReplicationRpcProvider(tlsOptions, newTestManifest(t))
	assert.Error(t, err)
}

func TestReplicationRpcProviderGetReplicateStreamInvalidTarget(t *testing.T) {
	provider := newTestProvider(t)

	stream, err := provider.GetReplicateStream(context.Background(), invalidTarget, constant.DefaultNamespace, 1, 1)
	assert.Error(t, err)
	assert.Nil(t, stream)
}

func TestReplicationRpcProviderSendSnapshotInvalidTarget(t *testing.T) {
	provider := newTestProvider(t)

	stream, err := provider.SendSnapshot(context.Background(), invalidTarget, constant.DefaultNamespace, 1, 1)
	assert.Error(t, err)
	assert.Nil(t, stream)
}

func TestReplicationRpcProviderTruncate(t *testing.T) {
	manifest := newTestManifest(t)
	require.NoError(t, manifest.SetInstanceID("instance-1"))
	provider, err := NewReplicationRpcProvider(&security.TLSOptions{}, manifest)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })

	instanceIDs := make(chan []string, 1)
	follower := startReplicationServer(t, &fakeReplicationServer{
		truncate: func(ctx context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			instanceIDs <- md.Get(constant.MetadataInstanceId)
			return &proto.TruncateResponse{
				HeadEntryId: &proto.EntryId{Term: req.Term, Offset: req.HeadEntryId.Offset - 1},
			}, nil
		},
	})

	response, err := provider.Truncate(follower, &proto.TruncateRequest{
		Namespace:   constant.DefaultNamespace,
		Shard:       1,
		Term:        2,
		HeadEntryId: &proto.EntryId{Term: 1, Offset: 10},
	})
	require.NoError(t, err)
	assert.True(t, pb.Equal(&proto.EntryId{Term: 2, Offset: 9}, response.HeadEntryId))
	assert.Equal(t, []string{"instance-1"}, <-instanceIDs)
}

func TestReplicationRpcProviderTruncateError(t *testing.T) {
	provider := newTestProvider(t)
	follower := startReplicationServer(t, &fakeReplicationServer{
		truncate: func(context.Context, *proto.TruncateRequest) (*proto.TruncateResponse, error) {
			return nil, constant.IntoGrpcStatusError(constant.ErrInvalidTerm)
		},
	})

	_, err := provider.Truncate(follower, &proto.TruncateRequest{Term: 1})
	assert.ErrorIs(t, err, constant.ErrInvalidTerm)
}

func TestReplicationRpcProviderTruncateInvalidTarget(t *testing.T) {
	provider := newTestProvider(t)

	response, err := provider.Truncate(invalidTarget, &proto.TruncateRequest{Term: 1})
	assert.Error(t, err)
	assert.Nil(t, response)
}

func newTestManifest(t *testing.T) *manifestpkg.Manifest {
	t.Helper()
	manifest, err := manifestpkg.NewManifest(t.TempDir())
	require.NoError(t, err)
	return manifest
}

func newTestProvider(t *testing.T) ReplicationRpcProvider {
	t.Helper()
	provider, err := NewReplicationRpcProvider(&security.TLSOptions{}, newTestManifest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	return provider
}

// startReplicationServer serves the replication service on a local port, and
// returns its address.
func startReplicationServer(t *testing.T, server proto.OxiaLogReplicationServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	proto.RegisterOxiaLogReplicationServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return listener.Addr().String()
}

type fakeReplicationServer struct {
	proto.UnimplementedOxiaLogReplicationServer
	truncate func(ctx context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error)
}

func (f *fakeReplicationServer) Truncate(ctx context.Context, req *proto.TruncateRequest) (
	*proto.TruncateResponse, error) {
	return f.truncate(ctx, req)
}
