// Copyright 2023 StreamNative, Inc.
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
	"errors"
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
	"math/rand"
	"oxia/oxia"
	"sync/atomic"
	"time"
)

type Config struct {
	ServiceAddr     string
	RequestRate     float64
	ReadPercentage  float64
	KeysCardinality uint32
	ValueSize       uint32

	BatchLinger         time.Duration
	MaxRequestsPerBatch int
	RequestTimeout      time.Duration
}

type Perf interface {
	Run(context.Context)
}

func New(config Config) Perf {
	return &perf{
		config: config,
	}
}

type perf struct {
	config    Config
	keys      []string
	failedOps atomic.Int64
}

func (p *perf) Run(ctx context.Context) {
	log.Info().
		Interface("config", p.config).
		Msg("Starting Oxia perf client")

	p.keys = make([]string, p.config.KeysCardinality)
	for i := uint32(0); i < p.config.KeysCardinality; i++ {
		p.keys[i] = fmt.Sprintf("key-%d", i)
	}

	client, err := oxia.NewAsyncClient(p.config.ServiceAddr,
		oxia.WithBatchLinger(p.config.BatchLinger),
		oxia.WithMaxRequestsPerBatch(p.config.MaxRequestsPerBatch),
		oxia.WithRequestTimeout(p.config.RequestTimeout),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Oxia client")
	}

	writeLatencyCh := make(chan int64)
	go p.generateWriteTraffic(ctx, client, writeLatencyCh)

	readLatencyCh := make(chan int64)
	go p.generateReadTraffic(ctx, client, readLatencyCh)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	wq := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	rq := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	writeOps := 0
	readOps := 0

	for {
		select {
		case <-ticker.C:
			writeRate := float64(writeOps) / float64(10)
			readRate := float64(readOps) / float64(10)
			failedOpsRate := float64(p.failedOps.Swap(0)) / float64(10)
			log.Info().Msgf(`Stats - Total ops: %6.1f ops/s - Failed ops: %6.1f ops/s
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
			)

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

		key := p.keys[rand.Intn(int(p.config.KeysCardinality))]

		start := time.Now()
		ch := client.Put(key, value)
		go func() {
			r := <-ch
			if r.Err != nil {
				log.Warn().Err(r.Err).
					Str("key", key).
					Msg("Operation has failed")
				p.failedOps.Add(1)
			} else {
				log.Debug().
					Str("key", key).
					Interface("version", r.Version).
					Msg("Operation has succeeded")

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

		key := p.keys[rand.Intn(int(p.config.KeysCardinality))]

		start := time.Now()
		ch := client.Get(key)
		go func() {
			r := <-ch
			if r.Err != nil && !errors.Is(r.Err, oxia.ErrorKeyNotFound) {
				log.Warn().Err(r.Err).
					Str("key", key).
					Msg("Operation has failed")
				p.failedOps.Add(1)
			} else {
				log.Debug().
					Str("key", key).
					Interface("version", r.Version).
					Msg("Operation has succeeded")

				latencyCh <- time.Since(start).Microseconds()
			}
		}()
	}
}
