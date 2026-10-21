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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

const testMethod = "/io.oxia.proto.OxiaClient/Read"

func TestNewGrpcInsIDVerifyInterceptorsUnaryChecksInstanceID(t *testing.T) {
	manifest := newTestManifest(t, "instance-1")
	tests := []struct {
		name     string
		ctx      context.Context
		expected codes.Code
	}{
		{"matching id", withInstanceIDs("instance-1"), codes.OK},
		{"unexpected id", withInstanceIDs("instance-2"), codes.PermissionDenied},
		{"no metadata", context.Background(), codes.Unauthenticated},
		{"no id", withInstanceIDs(), codes.Unauthenticated},
		{"empty id", withInstanceIDs(""), codes.InvalidArgument},
		{"repeated id", withInstanceIDs("instance-1", "instance-1"), codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handled, err := callUnary(tt.ctx, manifest, testMethod)

			assert.Equal(t, tt.expected, status.Code(err))
			assert.Equal(t, tt.expected == codes.OK, handled)
		})
	}
}

func TestNewGrpcInsIDVerifyInterceptorsUnarySkipsWhitelistedMethods(t *testing.T) {
	manifest := newTestManifest(t, "")
	for _, method := range []string{
		proto.OxiaCoordination_Handshake_FullMethodName,
		grpc_health_v1.Health_Check_FullMethodName,
	} {
		handled, err := callUnary(context.Background(), manifest, method)

		require.NoError(t, err)
		assert.True(t, handled)
	}
}

func TestNewGrpcInsIDVerifyInterceptorsUnaryRejectsUninitializedServer(t *testing.T) {
	manifest := newTestManifest(t, "")

	handled, err := callUnary(withInstanceIDs("instance-1"), manifest, testMethod)

	assert.ErrorContains(t, err, constant.ErrNotInitialized.Error())
	assert.False(t, handled)
}

func newTestManifest(t *testing.T, instanceID string) *manifestpkg.Manifest {
	t.Helper()
	manifest, err := manifestpkg.NewManifest(t.TempDir())
	require.NoError(t, err)
	if instanceID != "" {
		_, err = manifest.SetInstanceID(instanceID)
		require.NoError(t, err)
	}
	return manifest
}

func withInstanceIDs(ids ...string) context.Context {
	md := metadata.MD{}
	for _, id := range ids {
		md.Append(constant.MetadataInstanceId, id)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func callUnary(ctx context.Context, manifest *manifestpkg.Manifest, method string) (bool, error) {
	interceptor := NewGrpcInsIDVerifyInterceptors(manifest).Unary[0]
	handled := false
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method},
		func(context.Context, any) (any, error) {
			handled = true
			return "response", nil
		})
	return handled, err
}
