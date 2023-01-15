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

package oxia

import (
	"runtime"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/multierr"
)

const (
	DefaultBatchLinger         = 5 * time.Millisecond
	DefaultMaxRequestsPerBatch = 1000
	DefaultRequestTimeout      = 30 * time.Second
)

var (
	ErrorBatchLinger         = errors.New("BatchLinger must be greater than or equal to zero")
	ErrorMaxRequestsPerBatch = errors.New("MaxRequestsPerBatch must be greater than zero")
	ErrorRequestTimeout      = errors.New("RequestTimeout must be greater than zero")
	ErrorBatcherBufferSize   = errors.New("BatcherBufferSize must be greater than or equal to zero")
	DefaultBatcherBufferSize = runtime.GOMAXPROCS(-1)
)

// clientOptions contains options for the Oxia client.
type clientOptions struct {
	serviceAddress      string
	batchLinger         time.Duration
	maxRequestsPerBatch int
	requestTimeout      time.Duration
	meterProvider       metric.MeterProvider
	batcherBufferSize   int
}

func (o clientOptions) ServiceAddress() string {
	return o.serviceAddress
}

func (o clientOptions) BatchLinger() time.Duration {
	return o.batchLinger
}

func (o clientOptions) MaxRequestsPerBatch() int {
	return o.maxRequestsPerBatch
}

func (o clientOptions) BatcherBufferSize() int {
	return o.batcherBufferSize
}

// RequestTimeout defines how long the client will wait for responses before cancelling the request and failing
// the request.
func (o clientOptions) RequestTimeout() time.Duration {
	return o.requestTimeout
}

// ClientOption is an interface for applying Oxia client options.
type ClientOption interface {
	// apply is used to set a ClientOption value of a clientOptions.
	apply(option clientOptions) (clientOptions, error)
}

func newClientOptions(serviceAddress string, opts ...ClientOption) (clientOptions, error) {
	options := clientOptions{
		serviceAddress:      serviceAddress,
		batchLinger:         DefaultBatchLinger,
		maxRequestsPerBatch: DefaultMaxRequestsPerBatch,
		requestTimeout:      DefaultRequestTimeout,
		meterProvider:       metric.NewNoopMeterProvider(),
		batcherBufferSize:   DefaultBatcherBufferSize,
	}
	var errs error
	var err error
	for _, o := range opts {
		options, err = o.apply(options)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return options, errs
}

type clientOptionFunc func(clientOptions) (clientOptions, error)

func (f clientOptionFunc) apply(c clientOptions) (clientOptions, error) {
	return f(c)
}

// WithBatchLinger defines how long the batcher will wait before sending a batched request. The value must be greater
// than or equal to zero. A value of zero will disable linger, effectively disabling batching.
func WithBatchLinger(batchLinger time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if batchLinger < 0 {
			return options, ErrorBatchLinger
		}
		options.batchLinger = batchLinger
		return options, nil
	})
}

// WithMaxRequestsPerBatch defines how many individual requests a batch can contain before the batched request is sent.
// The value must be greater than zero. A value of one will effectively disable batching.
func WithMaxRequestsPerBatch(maxRequestsPerBatch int) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if maxRequestsPerBatch <= 0 {
			return options, ErrorMaxRequestsPerBatch
		}
		options.maxRequestsPerBatch = maxRequestsPerBatch
		return options, nil
	})
}

func WithRequestTimeout(requestTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if requestTimeout <= 0 {
			return options, ErrorRequestTimeout
		}
		options.requestTimeout = requestTimeout
		return options, nil
	})
}

func WithMeterProvider(meterProvider metric.MeterProvider) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if meterProvider == nil {
			options.meterProvider = metric.NewNoopMeterProvider()
		} else {
			options.meterProvider = meterProvider
		}
		return options, nil
	})
}

func WithGlobalMeterProvider() ClientOption {
	return WithMeterProvider(global.MeterProvider())
}

// WithBatcherBufferSize defines how many batch requests can be queued.
func WithBatcherBufferSize(batcherBufferSize int) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if batcherBufferSize < 0 {
			return options, ErrorBatcherBufferSize
		}
		options.batcherBufferSize = batcherBufferSize
		return options, nil
	})
}
