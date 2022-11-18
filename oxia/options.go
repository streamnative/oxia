package oxia

import "time"

type ClientOptions struct {
	serviceUrl   string
	batchLinger  time.Duration
	batchMaxSize int
	batchTimeout time.Duration
}

func NewClientOptions(serviceUrl string) ClientOptions {
	const (
		defaultBatchLinger  = 5 * time.Millisecond
		defaultBatchMaxSize = 1000
		defaultBatchTimeout = 30 * time.Second
	)
	return ClientOptions{
		serviceUrl:   serviceUrl,
		batchLinger:  defaultBatchLinger,
		batchMaxSize: defaultBatchMaxSize,
		batchTimeout: defaultBatchTimeout,
	}
}

func (o ClientOptions) BatchLinger(batchLinger time.Duration) ClientOptions {
	if batchLinger <= 0 {
		panic("BatchLinger must be greater than zero")
	}
	o.batchLinger = batchLinger
	return o
}

func (o ClientOptions) BatchMaxSize(batchMaxSize int) ClientOptions {
	if batchMaxSize <= 0 {
		panic("BatchMaxSize must be greater than zero")
	}
	o.batchMaxSize = batchMaxSize
	return o
}

func (o ClientOptions) BatchTimeout(batchTimeout time.Duration) ClientOptions {
	if batchTimeout <= 0 {
		panic("BatchTimeout must be greater than zero")
	}
	o.batchTimeout = batchTimeout
	return o
}
