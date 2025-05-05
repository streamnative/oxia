package workers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proofs"
)

var _ proofs.ProofWorker = &sequenceProof{}

type sequenceProof struct {
	*slog.Logger
	Client oxia.SyncClient

	sequenceKey      string
	nextBaseSequence atomic.Uint64
}

func (s *sequenceProof) Close() error {
	return nil
}

func (s *sequenceProof) Bootstrap(ctx context.Context) error {
	s.Info("bootstrap sequence proof worker")
	for {
		sequence := s.nextBaseSequence.Load()
		key, _, err := s.Client.Put(ctx, s.sequenceKey, []byte(fmt.Sprintf("%v", sequence)), oxia.SequenceKeysDeltas(1, 2))
		if err != nil {
			continue
		}
		parts := strings.Split(key, "-")
		if len(parts) != 3 {
			return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key format: %v", key))
		}

		for i := 1; i < 3; i++ {
			encodedDelta := parts[i]
			var delta uint64
			if _, err := fmt.Sscanf(encodedDelta, "%020d", &delta); err != nil {
				return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key delta format: %v", key))
			}
			expectDelta := sequence + uint64(i)
			if delta != expectDelta {
				return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key delta: %v   expect: %v  actual: %v", key, expectDelta, delta))
			}
		}

		s.nextBaseSequence.Add(1)
	}
}

func NewSequenceProof(Client oxia.SyncClient) proofs.ProofWorker {
	return &sequenceProof{
		Client:           Client,
		sequenceKey:      uuid.NewString(),
		nextBaseSequence: atomic.Uint64{},
		Logger: slog.With(
			slog.String("component", "sequence-proof"),
			slog.String("sequence-key", uuid.NewString())),
	}
}
