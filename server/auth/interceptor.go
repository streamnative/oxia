package auth

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log/slog"
	"strings"
)

const (
	MetadataAuthorizationKey = "authorization"
	TokenPrefix              = "Bearer "
)

var (
	ErrMetadataFetchFailed = errors.New("metadata fetch failed")
)

type GrpcAuthenticationDelegator struct {
	provider AuthenticationProvider

	validate func(ctx context.Context, provider AuthenticationProvider) (string, error)
}

func (delegator *GrpcAuthenticationDelegator) GetUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_, err := delegator.validate(ctx, delegator.provider)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, err.Error())
		}
		// todo: set username to metadata to support authorization
		return handler(ctx, req)
	}
}

func (delegator *GrpcAuthenticationDelegator) GetStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		_, err := delegator.validate(ss.Context(), delegator.provider)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, err.Error())
		}
		// todo: set username to metadata to support authorization
		return handler(srv, ss)
	}
}

func NewGrpcAuthenticationDelegator(provider AuthenticationProvider) (*GrpcAuthenticationDelegator, error) {
	delegator := &GrpcAuthenticationDelegator{
		provider: provider,
	}
	switch provider.AcceptParamType() {
	case ProviderParamTypeToken:
		delegator.validate = validateTokenWithContext
	default:
		return nil, ErrUnMatchedAuthenticationParamType
	}
	return delegator, nil
}

func validateTokenWithContext(ctx context.Context, provider AuthenticationProvider) (string, error) {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrMetadataFetchFailed
	}
	peerMeta, ok := peer.FromContext(ctx)
	if !ok {
		return "", ErrMetadataFetchFailed
	}
	val := meta.Get(MetadataAuthorizationKey)
	if len(val) < 1 {
		slog.Debug("Receive empty token from the client",
			slog.String("peer", peerMeta.Addr.String()))
		return "", ErrEmptyToken
	}
	token := strings.TrimPrefix(val[0], TokenPrefix)
	var userName string
	var err error
	if userName, err = provider.Authenticate(ctx, token); err != nil {
		slog.Debug("Failed to authenticate token",
			slog.String("peer", peerMeta.Addr.String()),
			slog.String("token", token))
		return "", err
	}
	return userName, nil
}
