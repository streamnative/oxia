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

// ClientOptions contains options for the Oxia client.
type ClientOptions struct {
	serviceAddress      string
	batchLinger         time.Duration
	maxRequestsPerBatch int
	requestTimeout      time.Duration
	meterProvider       metric.MeterProvider
	batcherBufferSize   int
}

// ServiceAddress is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
// shard assignments. Ideally this should be a load-balanced endpoint.
func (o ClientOptions) ServiceAddress() string {
	return o.serviceAddress
}

// BatchLinger defines how long the batcher will wait before sending a batched request. The value must be greater
// than or equal to zero. A value of zero will disable linger, effectively disabling batching.
func (o ClientOptions) BatchLinger() time.Duration {
	return o.batchLinger
}

// MaxRequestsPerBatch defines how many individual requests a batch can contain before the batched request is sent.
// The value must be greater than zero. A value of one will effectively disable batching.
func (o ClientOptions) MaxRequestsPerBatch() int {
	return o.maxRequestsPerBatch
}

// BatcherBufferSize defines how many batch requests can be queued.
func (o ClientOptions) BatcherBufferSize() int {
	return o.batcherBufferSize
}

// RequestTimeout defines how long the client will wait for responses before cancelling the request and failing
// the request.
func (o ClientOptions) RequestTimeout() time.Duration {
	return o.requestTimeout
}

// ClientOption is an interface for applying Oxia client options.
type ClientOption interface {
	// apply is used to set a ClientOption value of a ClientOptions.
	apply(option ClientOptions) (ClientOptions, error)
}

func NewClientOptions(serviceAddress string, opts ...ClientOption) (ClientOptions, error) {
	options := ClientOptions{
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

type clientOptionFunc func(ClientOptions) (ClientOptions, error)

func (f clientOptionFunc) apply(c ClientOptions) (ClientOptions, error) {
	return f(c)
}

func WithBatchLinger(batchLinger time.Duration) ClientOption {
	return clientOptionFunc(func(options ClientOptions) (ClientOptions, error) {
		if batchLinger < 0 {
			return options, ErrorBatchLinger
		}
		options.batchLinger = batchLinger
		return options, nil
	})
}

func WithMaxRequestsPerBatch(maxRequestsPerBatch int) ClientOption {
	return clientOptionFunc(func(options ClientOptions) (ClientOptions, error) {
		if maxRequestsPerBatch <= 0 {
			return options, ErrorMaxRequestsPerBatch
		}
		options.maxRequestsPerBatch = maxRequestsPerBatch
		return options, nil
	})
}

func WithBatchRequestTimeout(batchRequestTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options ClientOptions) (ClientOptions, error) {
		if batchRequestTimeout <= 0 {
			return options, ErrorRequestTimeout
		}
		options.requestTimeout = batchRequestTimeout
		return options, nil
	})
}

func WithMeterProvider(meterProvider metric.MeterProvider) ClientOption {
	return clientOptionFunc(func(options ClientOptions) (ClientOptions, error) {
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

func WithBatcherBufferSize(batcherBufferSize int) ClientOption {
	return clientOptionFunc(func(options ClientOptions) (ClientOptions, error) {
		if batcherBufferSize < 0 {
			return options, ErrorBatcherBufferSize
		}
		options.batcherBufferSize = batcherBufferSize
		return options, nil
	})
}
