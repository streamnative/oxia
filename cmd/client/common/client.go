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
	return oxia.NewAsyncClient(Config.ServiceAddr,
		oxia.WithBatchLinger(Config.BatchLinger),
		oxia.WithBatchRequestTimeout(Config.BatchRequestTimeoutSec),
		oxia.WithMaxRequestsPerBatch(Config.MaxRequestsPerBatch),
  )
}
