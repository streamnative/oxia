package auth

import (
	"context"

	"github.com/pkg/errors"
)

const (
	ProviderOIDC = "oidc"

	ProviderParamTypeToken = "token"
)

var (
	ErrUnsupportedProvider              = errors.New("unsupported authentication provider")
	ErrUnMatchedAuthenticationParamType = errors.New("unmatched authentication parameter type")
	ErrEmptyToken                       = errors.New("empty token")
	ErrMalformedToken                   = errors.New("malformed token")
)

var Disabled = Options{}

type Options struct {
	ProviderName   string
	ProviderParams string
}

func (op *Options) IsEnabled() bool {
	return op.ProviderName != ""
}

// todo: add metrics
type AuthenticationProvider interface {
	AcceptParamType() string
	Authenticate(ctx context.Context, param any) (string, error)
}

func NewAuthenticationProvider(ctx context.Context, options Options) (AuthenticationProvider, error) {
	switch options.ProviderName {
	case ProviderOIDC:
		return NewOIDCProvider(ctx, options.ProviderParams)
	default:
		return nil, ErrUnsupportedProvider
	}
}
