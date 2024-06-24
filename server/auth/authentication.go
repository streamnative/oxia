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
	ErrUnsupportedProvider              = errors.New("Unsupported authentication provider.")
	ErrUnMatchedAuthenticationParamType = errors.New("Unmatched authentication parameter type.")
	ErrEmptyToken                       = errors.New("Empty token")
	ErrMalformedToken                   = errors.New("Malformed token")
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
	Authenticate(ctx context.Context, param interface{}) (string, error)
}

func NewAuthenticationProvider(ctx context.Context, options Options) (AuthenticationProvider, error) {
	switch options.ProviderName {
	case ProviderOIDC:
		return NewOIDCProvider(ctx, options.ProviderParams)
	default:
		return nil, ErrUnsupportedProvider
	}
}
