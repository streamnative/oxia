package oxia

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewClientConfig(t *testing.T) {
	options, err := NewClientOptions("serviceAddress")
	assert.NoError(t, err)

	assert.Equal(t, "serviceAddress", options.ServiceAddress())
	assert.Equal(t, DefaultBatchLinger, options.BatchLinger())
	assert.Equal(t, DefaultMaxRequestsPerBatch, options.MaxRequestsPerBatch())
	assert.Equal(t, DefaultRequestTimeout, options.RequestTimeout())
}

func TestWithBatchLinger(t *testing.T) {
	for _, item := range []struct {
		batchLinger         time.Duration
		expectedBatchLinger time.Duration
		expectedErr         error
	}{
		{-1, DefaultBatchLinger, ErrorBatchLinger},
		{0, 0, nil},
		{1, 1, nil},
	} {
		options, err := NewClientOptions("serviceAddress", WithBatchLinger(item.batchLinger))
		assert.Equal(t, item.expectedBatchLinger, options.BatchLinger())
		assert.ErrorIs(t, err, item.expectedErr)
	}
}

func TestWithMaxRequestsPerBatch(t *testing.T) {
	for _, item := range []struct {
		maxRequestsPerBatch         int
		expectedMaxRequestsPerBatch int
		expectedErr                 error
	}{
		{-1, DefaultMaxRequestsPerBatch, ErrorMaxRequestsPerBatch},
		{0, DefaultMaxRequestsPerBatch, ErrorMaxRequestsPerBatch},
		{1, 1, nil},
	} {
		options, err := NewClientOptions("serviceAddress", WithMaxRequestsPerBatch(item.maxRequestsPerBatch))
		assert.Equal(t, item.expectedMaxRequestsPerBatch, options.MaxRequestsPerBatch())
		assert.ErrorIs(t, err, item.expectedErr)
	}
}

func TestWithBatchRequestTimeout(t *testing.T) {
	for _, item := range []struct {
		batchRequestTimeout         time.Duration
		expectedBatchRequestTimeout time.Duration
		expectedErr                 error
	}{
		{-1, DefaultRequestTimeout, ErrorRequestTimeout},
		{0, DefaultRequestTimeout, ErrorRequestTimeout},
		{1, 1, nil},
	} {
		options, err := NewClientOptions("serviceAddress", WithRequestTimeout(item.batchRequestTimeout))
		assert.Equal(t, item.expectedBatchRequestTimeout, options.RequestTimeout())
		assert.ErrorIs(t, err, item.expectedErr)
	}
}

func TestWithBatcherBufferSize(t *testing.T) {
	for _, item := range []struct {
		size         int
		expectedSize int
		expectedErr  error
	}{
		{-1, DefaultBatcherBufferSize, ErrorBatcherBufferSize},
		{0, 0, nil},
		{1, 1, nil},
	} {
		options, err := NewClientOptions("serviceAddress", WithBatcherBufferSize(item.size))
		assert.Equal(t, item.expectedSize, options.BatcherBufferSize())
		assert.ErrorIs(t, item.expectedErr, err)
	}
}
