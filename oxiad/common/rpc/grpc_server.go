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
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/rpc/grpcmetrics"
)

const (
	defaultGrpcServerKeepAliveMinTime        = 5 * time.Second
	defaultGrpcServerKeepPermitWithoutStream = true
	ReadinessProbeService                    = "oxia-readiness"
)

type GrpcServer interface {
	io.Closer

	Port() int
}

type Interceptors struct {
	Unary  []grpc.UnaryServerInterceptor
	Stream []grpc.StreamServerInterceptor
}

type GrpcProvider interface {
	StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar), tlsConf *tls.Config, options *auth.Options, interceptors *Interceptors) (GrpcServer, error)
}

var Default = &defaultProvider{}

type defaultProvider struct {
}

func (*defaultProvider) StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar), tlsConf *tls.Config, options *auth.Options, interceptors *Interceptors) (GrpcServer, error) {
	return newDefaultGrpcProvider(name, bindAddress, registerFunc, tlsConf, options, interceptors)
}

type defaultGrpcServer struct {
	io.Closer
	server *grpc.Server
	port   int
	log    *slog.Logger
	// serveDone is closed when Serve returns: the listener is closed by then
	serveDone chan struct{}
}

func newDefaultGrpcProvider(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar),
	tlsConf *tls.Config, authOptions *auth.Options, extraInterceptors *Interceptors) (GrpcServer, error) {
	tcs := insecure.NewCredentials()
	if tlsConf != nil {
		tcs = credentials.NewTLS(tlsConf)
	}
	streamInterceptors := []grpc.StreamServerInterceptor{
		grpcmetrics.StreamServerInterceptor,
	}
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		grpcmetrics.UnaryServerInterceptor,
	}
	if authOptions.IsEnabled() {
		provider, err := auth.NewAuthenticationProvider(context.Background(), *authOptions)
		if err != nil {
			slog.Error("Failed to init authentication provider",
				slog.Any("authOptions", *authOptions),
				slog.Any("error", err))
			return nil, err
		}
		delegator, err := auth.NewGrpcAuthenticationDelegator(provider)
		if err != nil {
			slog.Error("Failed to init grpc authentication delegator",
				slog.Any("authOptions", *authOptions),
				slog.Any("error", err))
			return nil, err
		}
		unaryInterceptors = append(unaryInterceptors, delegator.GetUnaryInterceptor())
		streamInterceptors = append(streamInterceptors, delegator.GetStreamInterceptor())
	}
	if extraInterceptors != nil {
		unaryInterceptors = append(unaryInterceptors, extraInterceptors.Unary...)
		streamInterceptors = append(streamInterceptors, extraInterceptors.Stream...)
	}

	c := &defaultGrpcServer{
		server: grpc.NewServer(
			grpc.Creds(tcs),
			grpc.ChainStreamInterceptor(streamInterceptors...),
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
			grpc.MaxRecvMsgSize(constant.MaxGrpcFrameSize),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             defaultGrpcServerKeepAliveMinTime,
				PermitWithoutStream: defaultGrpcServerKeepPermitWithoutStream,
			}),
		),
		serveDone: make(chan struct{}),
	}
	registerFunc(c.server)
	grpcmetrics.Register(c.server)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	c.port = listener.Addr().(*net.TCPAddr).Port

	c.log = slog.With(
		slog.String("grpc-server", name),
		slog.String("bindAddress", listener.Addr().String()),
	)

	go process.DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia": name,
			"bind": listener.Addr().String(),
		},
		func() {
			defer close(c.serveDone)
			// Serve returns ErrServerStopped when Close() stopped the server before
			// this goroutine was scheduled: a benign race, not a startup failure.
			if err := c.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				c.log.Error(
					"Failed to start serving",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
		},
	)

	c.log.Info("Started Grpc server")

	return c, nil
}

func (c *defaultGrpcServer) Port() int {
	return c.port
}

func (c *defaultGrpcServer) Close() error {
	c.server.GracefulStop()
	// GracefulStop closes the listener only if Serve has started; otherwise
	// Serve closes it when it starts. Wait for Serve, so that the port is
	// free when Close returns.
	<-c.serveDone
	c.log.Info("Stopped Grpc server")
	return nil
}
