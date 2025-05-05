package proofs

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

var (
	ErrUnexpectedResult = errors.New("proof: unexpected verification result")
)

type ProofWorker interface {
	io.Closer

	Bootstrap(ctx context.Context) error
}
