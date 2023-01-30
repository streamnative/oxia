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
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/multierr"
)

const (
	DefaultBatchLinger         = 5 * time.Millisecond
	DefaultMaxRequestsPerBatch = 1000
	DefaultRequestTimeout      = 30 * time.Second
	DefaultSessionTimeout      = 15 * time.Second
)

var (
	ErrorInvalidOptionBatchLinger         = errors.New("BatchLinger must be greater than or equal to zero")
	ErrorInvalidOptionMaxRequestsPerBatch = errors.New("MaxRequestsPerBatch must be greater than zero")
	ErrorInvalidOptionRequestTimeout      = errors.New("RequestTimeout must be greater than zero")
	ErrorInvalidOptionSessionTimeout      = errors.New("SessionTimeout must be greater than zero")
	ErrorInvalidOptionIdentity            = errors.New("Identity must be non-empty")
)

// clientOptions contains options for the Oxia client.
type clientOptions struct {
	serviceAddress      string
	batchLinger         time.Duration
	maxRequestsPerBatch int
	requestTimeout      time.Duration
	meterProvider       metric.MeterProvider
	sessionTimeout      time.Duration
	identity            string
}

func defaultIdentity() string {
	return uuid.NewString()
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
		sessionTimeout:      DefaultSessionTimeout,
		identity:            defaultIdentity(),
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
			return options, ErrorInvalidOptionBatchLinger
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
			return options, ErrorInvalidOptionMaxRequestsPerBatch
		}
		options.maxRequestsPerBatch = maxRequestsPerBatch
		return options, nil
	})
}

func WithRequestTimeout(requestTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if requestTimeout <= 0 {
			return options, ErrorInvalidOptionRequestTimeout
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

// WithGlobalMeterProvider instructs the Oxia client to use the global OpenTelemetry MeterProvider.
func WithGlobalMeterProvider() ClientOption {
	return WithMeterProvider(global.MeterProvider())
}

// WithSessionTimeout specifies the session timeout to
func WithSessionTimeout(sessionTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if sessionTimeout <= 0 {
			return options, ErrorInvalidOptionSessionTimeout
		}
		options.sessionTimeout = sessionTimeout
		return options, nil
	})
}

func WithIdentity(identity string) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if identity == "" {
			return options, ErrorInvalidOptionIdentity
		}
		options.identity = identity
		return options, nil
	})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type putOptions struct {
	expectedVersion *int64
	ephemeral       bool
}

// PutOption represents an option for the [SyncClient.Put] operation
type PutOption interface {
	applyPut(opts putOptions) putOptions
}

func newPutOptions(opts []PutOption) putOptions {
	putOpts := putOptions{}
	for _, opt := range opts {
		putOpts = opt.applyPut(putOpts)
	}
	return putOpts
}

// ExpectedRecordNotExists Marks that the put operation should only be successful
// if the record does not exist yet.
func ExpectedRecordNotExists() PutOption {
	return &expectedVersionId{VersionIdNotExists}
}

type deleteOptions struct {
	expectedVersion *int64
}

// DeleteOption represents an option for the [SyncClient.Delete] operation
type DeleteOption interface {
	PutOption
	applyDelete(opts deleteOptions) deleteOptions
}

func newDeleteOptions(opts []DeleteOption) deleteOptions {
	deleteOpts := deleteOptions{}
	for _, opt := range opts {
		deleteOpts = opt.applyDelete(deleteOpts)
	}
	return deleteOpts
}

// ExpectedVersionId Marks that the operation should only be successful
// if the versionId of the record stored in the server matches the expected one
func ExpectedVersionId(versionId int64) DeleteOption {
	return &expectedVersionId{versionId}
}

type expectedVersionId struct {
	versionId int64
}

func (e *expectedVersionId) applyPut(opts putOptions) putOptions {
	opts.expectedVersion = &e.versionId
	return opts
}

func (e *expectedVersionId) applyDelete(opts deleteOptions) deleteOptions {
	opts.expectedVersion = &e.versionId
	return opts
}

type ephemeral struct{}

var ephemeralFlag = &ephemeral{}

func (e *ephemeral) applyPut(opts putOptions) putOptions {
	opts.ephemeral = true
	return opts
}

// Ephemeral marks the record to be created as an ephemeral record.
// Ephemeral records have their lifecycle tied to a particular client instance, and they
// are automatically deleted when the client instance is closed.
// These records are also deleted if the client cannot communicate with the Oxia
// service for some extended amount of time, and the session between the client and
// the service "expires"
// Application can control the session behavior by setting the session timeout
// appropriately with [WithSessionTimeout] option when creating the client instance.
func Ephemeral() PutOption {
	return ephemeralFlag
}
