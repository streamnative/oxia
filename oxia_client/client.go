package oxia_client

import (
	"oxia/internal/client/impl"
	"oxia/oxia"
)

func New(options *oxia.Options) oxia.Client {
	if options.InMemory {
		return impl.NewMemoryClient()
	}
	return impl.NewClient(options)
}
