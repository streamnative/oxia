package workers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proofs"
	"golang.org/x/time/rate"
)

var _ proofs.ProofWorker = &sequenceProof{}

type sequenceProof struct {
	*slog.Logger
	Client oxia.SyncClient

	sequenceKey      string
	nextBaseSequence atomic.Uint64
	ratePerSec       uint64
}

func (s *sequenceProof) Close() error {
	return nil
}

func (s *sequenceProof) Bootstrap(ctx context.Context) error {
	s.Info("bootstrap sequence proof worker")
	limiter := rate.NewLimiter(rate.Limit(s.ratePerSec), 1)
	for {
		if err := limiter.Wait(ctx); err != nil {
			s.Error("unexpected error when wait for rate limiter", slog.Any("error", err))
			return nil
		}
		sequence := s.nextBaseSequence.Load()
		key, _, err := s.Client.Put(ctx, s.sequenceKey, []byte(fmt.Sprintf("%v", sequence)), oxia.SequenceKeysDeltas(1, 2), oxia.PartitionKey(s.sequenceKey))
		if err != nil {
			s.Warn("receive error when put with sequence", slog.Any("error", err), slog.Uint64("sequence", sequence))
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
			expectDelta := (sequence + 1) * uint64(i)
			if delta > expectDelta {
				// it's expected. because the server might crash without response return sometimes.
				// todo: add extra validation to check if the value is expected.
				slog.Warn("receive a sequence key with a delta larger than expected. But it's okay. It might be caused by the server crash. (data store,d but the response has not finished)", slog.String("key", key), slog.Uint64("expect", expectDelta), slog.Uint64("actual", delta))
				continue
			}
			if delta < expectDelta {
				return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key delta: %v   expect: %v  actual: %v", key, expectDelta, delta))
			}
		}

		s.nextBaseSequence.Add(1)

		if sequence%1000 == 0 {
			s.Info("sent sequence", slog.Uint64("sequence", sequence))
		}
	}
}

func NewSequenceProof(Client oxia.SyncClient) proofs.ProofWorker {
	sequenceKey := fmt.Sprintf("%d", time.Now().UnixMilli())
	return &sequenceProof{
		Client:           Client,
		sequenceKey:      sequenceKey,
		nextBaseSequence: atomic.Uint64{},
		ratePerSec:       1000,
		Logger: slog.With(
			slog.String("component", "sequence-proof"),
			slog.String("sequence-key", sequenceKey)),
	}
}
