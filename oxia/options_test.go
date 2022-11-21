package oxia

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewClientConfig(t *testing.T) {
	options, err := NewClientOptions("serviceUrl")
	assert.ErrorIs(t, nil, err)

	assert.Equal(t, "serviceUrl", options.ServiceUrl())
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
		options, err := NewClientOptions("serviceUrl", WithBatchLinger(item.batchLinger))
		fmt.Println(options)
		assert.Equal(t, item.expectedBatchLinger, options.BatchLinger())
		assert.ErrorIs(t, item.expectedErr, err)
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
		options, err := NewClientOptions("serviceUrl", WithMaxRequestsPerBatch(item.maxRequestsPerBatch))
		fmt.Println(options)
		assert.Equal(t, item.expectedMaxRequestsPerBatch, options.MaxRequestsPerBatch())
		assert.ErrorIs(t, item.expectedErr, err)
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
		options, err := NewClientOptions("serviceUrl", WithBatchRequestTimeout(item.batchRequestTimeout))
		fmt.Println(options)
		assert.Equal(t, item.expectedBatchRequestTimeout, options.BatchRequestTimeout())
		assert.ErrorIs(t, item.expectedErr, err)
	}
}
