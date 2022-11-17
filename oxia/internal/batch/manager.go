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

func (m *Manager) Get(shardId uint32) Batcher {
	//double-check lock
	batcher, ok := m.batchers[shardId]
	if !ok {
		m.Lock()
		if batcher, ok = m.batchers[shardId]; !ok {
			batcher = m.batcherFactory(&shardId)
			m.batchers[shardId] = batcher
		}
		m.Unlock()
	}
	return batcher
}

func (m *Manager) Close() error {
	var errs error
	m.Lock()
	for id, batcher := range m.batchers {
		delete(m.batchers, id)
		if err := batcher.Close(); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	m.Unlock()
	return errs
}
