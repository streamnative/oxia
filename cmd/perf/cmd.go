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
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"io"
	"math/rand"
	"oxia/common"
	"oxia/kubernetes"
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
	BatcherBufferSize   int
}

var (
	Cmd = &cobra.Command{
		Use:   "perf",
		Short: "Oxia perf client",
		Long:  `Oxia tool for basic performance tests`,
		Run:   exec,
	}

	config    = &Config{}
	keys      []string
	failedOps atomic.Int64
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", kubernetes.PublicPort.Port)
	Cmd.Flags().StringVarP(&config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")

	Cmd.Flags().Float64VarP(&config.RequestRate, "rate", "r", 100.0, "Request rate, ops/s")
	Cmd.Flags().Float64VarP(&config.ReadPercentage, "read-write-percent", "p", 80.0, "Percentage of read requests, compared to total requests")
	Cmd.Flags().Uint32Var(&config.KeysCardinality, "keys-cardinality", 1000, "Batch linger in milliseconds")
	Cmd.Flags().Uint32VarP(&config.ValueSize, "value-size", "s", 128, "Size of the values to write")

	Cmd.Flags().DurationVar(&config.BatchLinger, "batch-linger", oxia.DefaultBatchLinger, "Batch linger time")
	Cmd.Flags().IntVar(&config.MaxRequestsPerBatch, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "Maximum requests per batch")
	Cmd.Flags().DurationVar(&config.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Request timeout")
	Cmd.Flags().IntVar(&config.BatcherBufferSize, "batcher-buffer-size", oxia.DefaultBatcherBufferSize, "Batcher buffer size")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(runPerf)
}

type closer struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newCloser() *closer {
	c := &closer{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *closer) Close() error {
	c.cancel()
	return nil
}

func runPerf() (io.Closer, error) {
	closer := newCloser()
	go perfMain(closer)
	return closer, nil
}

func perfMain(closer *closer) {
	defer closer.cancel()

	log.Info().
		Interface("config", config).
		Msg("Starting Oxia perf client")

	keys = make([]string, config.KeysCardinality)
	for i := uint32(0); i < config.KeysCardinality; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	client, err := oxia.NewAsyncClient(config.ServiceAddr,
		oxia.WithBatchLinger(config.BatchLinger),
		oxia.WithMaxRequestsPerBatch(config.MaxRequestsPerBatch),
		oxia.WithRequestTimeout(config.RequestTimeout),
		oxia.WithBatcherBufferSize(config.BatcherBufferSize),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Oxia client")
	}

	writeLatencyCh := make(chan int64)
	go generateWriteTraffic(closer, client, writeLatencyCh)

	readLatencyCh := make(chan int64)
	go generateReadTraffic(closer, client, readLatencyCh)

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
			failedOpsRate := float64(failedOps.Swap(0)) / float64(10)
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

		case <-closer.ctx.Done():
			return
		}
	}
}

func generateWriteTraffic(closer *closer, client oxia.AsyncClient, latencyCh chan int64) {
	writeRate := config.RequestRate * (100.0 - config.ReadPercentage) / 100
	limiter := rate.NewLimiter(rate.Limit(writeRate), int(writeRate))

	value := make([]byte, config.ValueSize)

	for {
		if err := limiter.Wait(closer.ctx); err != nil {
			return
		}

		key := keys[rand.Intn(int(config.KeysCardinality))]

		start := time.Now()
		ch := client.Put(key, value)
		go func() {
			r := <-ch
			if r.Err != nil {
				log.Warn().Err(r.Err).
					Str("key", key).
					Msg("Operation has failed")
				failedOps.Add(1)
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

func generateReadTraffic(closer *closer, client oxia.AsyncClient, latencyCh chan int64) {
	readRate := config.RequestRate * config.ReadPercentage / 100
	limiter := rate.NewLimiter(rate.Limit(readRate), int(readRate))

	for {
		if err := limiter.Wait(closer.ctx); err != nil {
			return
		}

		key := keys[rand.Intn(int(config.KeysCardinality))]

		start := time.Now()
		ch := client.Get(key)
		go func() {
			r := <-ch
			if r.Err != nil && !errors.Is(r.Err, oxia.ErrorKeyNotFound) {
				log.Warn().Err(r.Err).
					Str("key", key).
					Msg("Operation has failed")
				failedOps.Add(1)
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
