package batch

import (
	"go.uber.org/multierr"
	"sync"
)

func NewManager(batcherFactory func(*uint32) Batcher) *Manager {
	return &Manager{
		batcherFactory: batcherFactory,
		batchers:       make(map[uint32]Batcher),
	}
}

//////////

type Manager struct {
	sync.Mutex
	batcherFactory func(*uint32) Batcher
	batchers       map[uint32]Batcher
}

func (b *Manager) Get(shardId uint32) Batcher {
	//double-check lock
	batcher, ok := b.batchers[shardId]
	if !ok {
		b.Lock()
		if batcher, ok = b.batchers[shardId]; !ok {
			batcher = b.batcherFactory(&shardId)
			b.batchers[shardId] = batcher
		}
		b.Unlock()
	}
	return batcher
}

func (b *Manager) Close() error {
	var errs error
	for id, batcher := range b.batchers {
		delete(b.batchers, id)
		if err := batcher.Close(); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}
