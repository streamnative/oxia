// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/common/container"
	"testing"
)

func TestInternalHealthCheck(t *testing.T) {
	healthServer := health.NewServer()
	server, err := newInternalRpcServer(container.Default, "localhost:0", nil,
		NewShardAssignmentDispatcher(healthServer), healthServer)
	assert.NoError(t, err)

	target := fmt.Sprintf("localhost:%d", server.grpcServer.Port())
	cnx, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	client := grpc_health_v1.NewHealthClient(cnx)

	request := &grpc_health_v1.HealthCheckRequest{Service: ""}
	response, err := client.Check(context.Background(), request)
	assert.NoError(t, err)

	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, response.Status)
}
