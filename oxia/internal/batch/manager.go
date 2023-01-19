// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"go.uber.org/multierr"
	"oxia/common/batch"
	"sync"
)

func NewManager(batcherFactory func(*uint32) batch.Batcher) *Manager {
	return &Manager{
		batcherFactory: batcherFactory,
		batchers:       make(map[uint32]batch.Batcher),
	}
}

//////////

type Manager struct {
	sync.Mutex
	batcherFactory func(*uint32) batch.Batcher
	batchers       map[uint32]batch.Batcher
}

func (m *Manager) Get(shardId uint32) batch.Batcher {
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
