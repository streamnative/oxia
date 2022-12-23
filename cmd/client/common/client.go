package common

import (
	"oxia/oxia"
	"time"
)

var (
	Config = ClientConfig{}
)

type ClientConfig struct {
	ServiceAddr            string
	BatchLingerMs          int
	MaxRequestsPerBatch    int
	BatchRequestTimeoutSec int
	BatcherBufferSize      int
}

func (config *ClientConfig) NewClient() (oxia.AsyncClient, error) {
	return oxia.NewAsyncClient(Config.ServiceAddr,
		oxia.WithBatchLinger(time.Duration(Config.BatchLingerMs)*time.Millisecond),
		oxia.WithBatchRequestTimeout(time.Duration(Config.BatchRequestTimeoutSec)*time.Second),
		oxia.WithMaxRequestsPerBatch(Config.MaxRequestsPerBatch))
}
