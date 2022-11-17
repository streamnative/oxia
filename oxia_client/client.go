package oxia_client

import (
	"oxia/internal/client/impl"
	"oxia/oxia"
)

type Options struct {
	InMemory bool
}

func New(options *Options) oxia.Client {
	if options.InMemory {
		return impl.NewMemoryClient()
	}
	panic("not yet implemented")
}
