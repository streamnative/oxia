// Copyright 2025 StreamNative, Inc.
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

package workers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proofs"
)

var _ proofs.ProofWorker = &sequenceProof{}

type sequenceProof struct {
	*slog.Logger
	Client oxia.SyncClient

	sequenceKey   string
	nextBaseDelta atomic.Uint64
	ratePerSec    uint64
}

func (*sequenceProof) Close() error {
	return nil
}

func (s *sequenceProof) Bootstrap(ctx context.Context) error {
	s.Info("bootstrap sequence proof worker")
	limiter := rate.NewLimiter(rate.Limit(s.ratePerSec), 1)

nextPut:
	for {
		if err := limiter.Wait(ctx); err != nil {
			s.Error("unexpected error when wait for rate limiter", slog.Any("error", err))
			return nil
		}
		sequence := s.nextBaseDelta.Load()
		key, _, err := s.Client.Put(ctx, s.sequenceKey, []byte(fmt.Sprintf("%v", sequence)), oxia.SequenceKeysDeltas(1, 2), oxia.PartitionKey(s.sequenceKey))
		if err != nil {
			s.Warn("receive error when put with sequence", slog.Any("error", err), slog.Uint64("sequence", sequence))
			continue
		}
		parts := strings.Split(key, "-")
		if len(parts) != 3 {
			return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key format: %v", key))
		}

		for deltaIndex := 1; deltaIndex < 3; deltaIndex++ {
			encodedDelta := parts[deltaIndex]
			var delta uint64
			if _, err := fmt.Sscanf(encodedDelta, "%020d", &delta); err != nil {
				return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key delta format: %v", key))
			}
			expectDelta := (sequence + 1) * uint64(deltaIndex)
			if delta > expectDelta {
				// it's expected. because the server might crash without response return sometimes.
				// todo: add extra validation to check if the value is expected.
				slog.Warn("receive a sequence key with a delta larger than expected. But it's okay. It might be caused by the server crash. (data stored but the response has not finished)", slog.String("key", key), slog.Uint64("expect", expectDelta), slog.Uint64("actual", delta))
				if deltaIndex == 1 { // base delta
					s.nextBaseDelta.Store(delta)
				}
				continue nextPut
			}
			if delta < expectDelta {
				return errors.Wrap(proofs.ErrUnexpectedResult, fmt.Sprintf("unexpected sequence key delta: %v   expect: %v  actual: %v", key, expectDelta, delta))
			}
		}

		s.nextBaseDelta.Add(1)

		if sequence%1000 == 0 {
			s.Info("sent sequence", slog.Uint64("sequence", sequence))
		}
	}
}

func NewSequenceProof(client oxia.SyncClient) proofs.ProofWorker {
	sequenceKey := fmt.Sprintf("%d", time.Now().UnixMilli())
	return &sequenceProof{
		Client:        client,
		sequenceKey:   sequenceKey,
		nextBaseDelta: atomic.Uint64{},
		ratePerSec:    1000,
		Logger: slog.With(
			slog.String("component", "sequence-proof"),
			slog.String("sequence-key", sequenceKey)),
	}
}
