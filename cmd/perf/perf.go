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

package perf

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/bmizerany/perks/quantile"
	"golang.org/x/time/rate"

	"github.com/oxia-db/oxia/oxia"
)

const DefaultStatsInterval = 10 * time.Second

type Config struct {
	ServiceAddr     string
	Namespace       string
	RequestRate     float64
	ReadPercentage  float64
	KeysCardinality uint32
	ValueSize       uint32

	RandomPayload bool

	BatchLinger         time.Duration
	MaxRequestsPerBatch int
	RequestTimeout      time.Duration

	// StatsInterval is the interval between the stats reports. Zero means use
	// DefaultStatsInterval.
	StatsInterval time.Duration
}

type Perf interface {
	Run(context.Context)
}

func New(config Config) (Perf, error) {
	slog.Info(
		"Starting Oxia perf client",
		slog.Any("config", config),
	)

	client, err := oxia.NewAsyncClient(config.ServiceAddr,
		oxia.WithNamespace(config.Namespace),
		oxia.WithBatchLinger(config.BatchLinger),
		oxia.WithMaxRequestsPerBatch(config.MaxRequestsPerBatch),
		oxia.WithRequestTimeout(config.RequestTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Oxia client: %w", err)
	}

	keys := make([]string, config.KeysCardinality)
	for i := uint32(0); i < config.KeysCardinality; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return &perf{
		config: config,
		client: client,
		keys:   keys,
	}, nil
}

type perf struct {
	config    Config
	client    oxia.AsyncClient
	keys      []string
	failedOps atomic.Int64
}

// Run sends the traffic until the context is done, then closes the client.
func (p *perf) Run(ctx context.Context) {
	defer func() {
		if err := p.client.Close(); err != nil {
			slog.Warn("Failed to close Oxia client", slog.Any("error", err))
		}
	}()

	writeLatencyCh := make(chan int64)
	go p.generateWriteTraffic(ctx, p.client, writeLatencyCh)

	readLatencyCh := make(chan int64)
	go p.generateReadTraffic(ctx, p.client, readLatencyCh)

	statsInterval := p.config.StatsInterval
	if statsInterval == 0 {
		statsInterval = DefaultStatsInterval
	}
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	wq := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	rq := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	writeOps := 0
	readOps := 0

	for {
		select {
		case <-ticker.C:
			seconds := statsInterval.Seconds()
			writeRate := float64(writeOps) / seconds
			readRate := float64(readOps) / seconds
			failedOpsRate := float64(p.failedOps.Swap(0)) / seconds
			slog.Info(fmt.Sprintf(`Stats - Total ops: %6.1f ops/s - Failed ops: %6.1f ops/s
			Write ops %6.1f w/s  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f
			Read  ops %6.1f r/s  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				writeRate+readRate,
				failedOpsRate,
				writeRate,
				wq.Query(0.5),
				wq.Query(0.95),
				wq.Query(0.99),
				wq.Query(0.999),
				wq.Query(1.0),
				readRate,
				rq.Query(0.5),
				rq.Query(0.95),
				rq.Query(0.99),
				rq.Query(0.999),
				rq.Query(1.0),
			))

			wq.Reset()
			rq.Reset()
			writeOps = 0
			readOps = 0

		case wl := <-writeLatencyCh:
			writeOps++
			wq.Insert(float64(wl) / 1000.0) // Convert to millis

		case rl := <-readLatencyCh:
			readOps++
			rq.Insert(float64(rl) / 1000.0) // Convert to millis

		case <-ctx.Done():
			return
		}
	}
}

func (p *perf) generateWriteTraffic(ctx context.Context, client oxia.AsyncClient, latencyCh chan int64) {
	writeRate := p.config.RequestRate * (100.0 - p.config.ReadPercentage) / 100
	limiter := rate.NewLimiter(rate.Limit(writeRate), int(writeRate))

	value := make([]byte, p.config.ValueSize)

	for {
		if err := limiter.Wait(ctx); err != nil {
			return
		}

		if p.config.RandomPayload {
			// The previous Put can still be sending its value
			value = make([]byte, p.config.ValueSize)
			for i := 0; i+8 <= len(value); i += 8 {
				binary.NativeEndian.PutUint64(value[i:], rand.Uint64()) //nolint:gosec
			}
		}

		key := p.keys[rand.Intn(int(p.config.KeysCardinality))] //nolint:gosec

		start := time.Now()
		ch := client.Put(key, value)
		go func() {
			r := <-ch
			if r.Err != nil {
				slog.Warn(
					"Write operation has failed",
					slog.Any("error", r.Err),
					slog.String("key", key),
				)
				p.failedOps.Add(1)
			} else {
				slog.Debug(
					"Write operation has succeeded",
					slog.String("key", key),
					slog.Any("version", r.Version),
				)

				latencyCh <- time.Since(start).Microseconds()
			}
		}()
	}
}

func (p *perf) generateReadTraffic(ctx context.Context, client oxia.AsyncClient, latencyCh chan int64) {
	readRate := p.config.RequestRate * p.config.ReadPercentage / 100
	limiter := rate.NewLimiter(rate.Limit(readRate), int(readRate))

	for {
		if err := limiter.Wait(ctx); err != nil {
			return
		}

		key := p.keys[rand.Intn(int(p.config.KeysCardinality))] //nolint:gosec

		start := time.Now()
		ch := client.Get(key)
		go func() {
			r := <-ch
			if r.Err != nil && !errors.Is(r.Err, oxia.ErrKeyNotFound) {
				slog.Warn(
					"Read operation has failed",
					slog.Any("error", r.Err),
					slog.String("key", key),
				)
				p.failedOps.Add(1)
			} else {
				slog.Debug(
					"Read operation has succeeded",
					slog.String("key", key),
					slog.Any("version", r.Version),
				)

				latencyCh <- time.Since(start).Microseconds()
			}
		}()
	}
}
