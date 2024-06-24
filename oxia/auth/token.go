package auth

import (
	"context"
)

type tokenAuthentication struct {
	requireTransportSecurity bool
	tokenGetFunc             func() string
}

func (tokenAuth *tokenAuthentication) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	token := tokenAuth.tokenGetFunc()
	return map[string]string{
		"authorization": "Bearer" + " " + token,
	}, nil
}

func (tokenAuth *tokenAuthentication) RequireTransportSecurity() bool {
	return tokenAuth.requireTransportSecurity
}

func NewTokenAuthenticationWithFunc(tokenGetFunc func() string, requireTransportSecurity bool) Authentication {
	return &tokenAuthentication{tokenGetFunc: tokenGetFunc, requireTransportSecurity: requireTransportSecurity}
}

func NewTokenAuthenticationWithToken(token string, requireTransportSecurity bool) Authentication {
	return NewTokenAuthenticationWithFunc(func() string {
		return token
	}, requireTransportSecurity)
}
