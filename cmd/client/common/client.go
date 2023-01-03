package common

import (
	"oxia/oxia"
	"time"
)

var (
	Config = ClientConfig{}
)

type ClientConfig struct {
	ServiceAddr         string
	BatchLinger         time.Duration
	MaxRequestsPerBatch int
	BatchRequestTimeout time.Duration
	BatcherBufferSize   int
}

func (config *ClientConfig) NewClient() (oxia.AsyncClient, error) {
	options, err := oxia.NewClientOptions(Config.ServiceAddr,
		oxia.WithBatchLinger(Config.BatchLinger),
		oxia.WithBatchRequestTimeout(Config.BatchRequestTimeout),
		oxia.WithMaxRequestsPerBatch(Config.MaxRequestsPerBatch),
	)
	if err != nil {
		return nil, err
	}
	return oxia.NewAsyncClient(options), nil
}
