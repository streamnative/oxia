package oxia

import (
	"github.com/pkg/errors"
	"time"
)

type ClientOptions struct {
	/**
	 * ServiceUrl is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
	 * shard assignments. Ideally this should be a load-balanced endpoint.
	 */
	ServiceUrl string
	/**
	 * BatchLinger defines how long the batcher will wait before sending a batched request. The value must be greater
	 * than or equal to zero. A value of zero will disable linger, effectively disabling batching.
	 */
	BatchLinger time.Duration
	/**
	 * MaxRequestsPerBatch defines how many individual requests a batch can contain before the batched request is sent.
	 * The value must be greater than zero. A value of one will effectively disable batching.
	 */
	MaxRequestsPerBatch int
	/**
	 * BatchRequestTimeout defines how long the client will wait for responses before cancelling the request and failing
	 * the batch.
	 */
	BatchRequestTimeout time.Duration
}

func NewClientOptions(serviceUrl string) ClientOptions {
	const (
		defaultBatchLinger         = 5 * time.Millisecond
		defaultMaxRequestsPerBatch = 1000
		defaultBatchRequestTimeout = 30 * time.Second
	)
	return ClientOptions{
		ServiceUrl:          serviceUrl,
		BatchLinger:         defaultBatchLinger,
		MaxRequestsPerBatch: defaultMaxRequestsPerBatch,
		BatchRequestTimeout: defaultBatchRequestTimeout,
	}
}

func (o *ClientOptions) Validate() error {
	if o.BatchLinger < 0 {
		return errors.New("BatchLinger must be greater than or equal to zero")
	}
	if o.MaxRequestsPerBatch <= 0 {
		return errors.New("MaxRequestsPerBatch must be greater than zero")
	}
	if o.BatchRequestTimeout <= 0 {
		return errors.New("BatchRequestTimeout must be greater than zero")
	}
	return nil
}
