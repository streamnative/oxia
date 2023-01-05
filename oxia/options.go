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
	DefaultBatchRequestTimeout = 30 * time.Second
)

var (
	ErrorBatchLinger         = errors.New("BatchLinger must be greater than or equal to zero")
	ErrorMaxRequestsPerBatch = errors.New("MaxRequestsPerBatch must be greater than zero")
	ErrorBatchRequestTimeout = errors.New("BatchRequestTimeout must be greater than zero")
	ErrorBatcherBuffereSize  = errors.New("BatcherBufferSize must be greater than or equal to zero")
	DefaultBatcherBufferSize = runtime.GOMAXPROCS(-1)
)

// clientOptions contains options for the Oxia client.
type clientOptions struct {
	serviceAddress      string
	batchLinger         time.Duration
	maxRequestsPerBatch int
	batchRequestTimeout time.Duration
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

func (o clientOptions) BatchRequestTimeout() time.Duration {
	return o.batchRequestTimeout
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
		batchRequestTimeout: DefaultBatchRequestTimeout,
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

// WithBatchRequestTimeout defines how long the client will wait for responses before cancelling the request and failing
// the batch.
func WithBatchRequestTimeout(batchRequestTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if batchRequestTimeout <= 0 {
			return options, ErrorBatchRequestTimeout
		}
		options.batchRequestTimeout = batchRequestTimeout
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
			return options, ErrorBatcherBuffereSize
		}
		options.batcherBufferSize = batcherBufferSize
		return options, nil
	})
}
