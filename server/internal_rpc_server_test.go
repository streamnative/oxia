package server

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"testing"
)

func TestInternalHealthCheck(t *testing.T) {
	server, err := newCoordinationRpcServer("localhost:0", nil, NewShardAssignmentDispatcher())
	assert.NoError(t, err)

	target := fmt.Sprintf("localhost:%d", server.container.Port())
	cnx, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	client := grpc_health_v1.NewHealthClient(cnx)

	request := &grpc_health_v1.HealthCheckRequest{Service: ""}
	response, err := client.Check(context.Background(), request)
	assert.NoError(t, err)

	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, response.Status)
}
