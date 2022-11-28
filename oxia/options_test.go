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
	assert.Equal(t, DefaultBatchRequestTimeout, options.BatchRequestTimeout())
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
		{-1, DefaultBatchRequestTimeout, ErrorBatchRequestTimeout},
		{0, DefaultBatchRequestTimeout, ErrorBatchRequestTimeout},
		{1, 1, nil},
	} {
		options, err := NewClientOptions("serviceAddress", WithBatchRequestTimeout(item.batchRequestTimeout))
		assert.Equal(t, item.expectedBatchRequestTimeout, options.BatchRequestTimeout())
		assert.ErrorIs(t, err, item.expectedErr)
	}
}
