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

package coordinator

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/streamnative/oxia/common/rpc"

	"github.com/streamnative/oxia/server/auth"
)

type rpcServer struct {
	grpcServer   rpc.GrpcServer
	healthServer *health.Server
}

func newRpcServer(bindAddress string, tlsConf *tls.Config) (*rpcServer, error) {
	server := &rpcServer{
		healthServer: health.NewServer(),
	}

	var err error
	server.grpcServer, err = rpc.Default.StartGrpcServer("coordinator", bindAddress, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, server.healthServer)
	}, tlsConf, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *rpcServer) Close() error {
	s.healthServer.Shutdown()
	return s.grpcServer.Close()
}
