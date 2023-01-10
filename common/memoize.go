package common

import (
	"sync"
	"time"
)

type memoize[T any] struct {
	sync.RWMutex

	provider    func() T
	cachedValue T
	lastCalled  time.Time
	cacheTime   time.Duration
}

// Memoize is used to cache the result of the invocation of a function
// for a certain amount of time
func Memoize[T any](provider func() T, cacheTime time.Duration) func() T {
	m := memoize[T]{
		provider:  provider,
		cacheTime: cacheTime,
	}

	return func() T {
		return m.Get()
	}
}

func (m *memoize[T]) Get() T {
	m.RLock()

	if time.Since(m.lastCalled) < m.cacheTime {
		defer m.RUnlock()
		return m.cachedValue
	}

	m.RUnlock()

	m.Lock()
	defer m.Unlock()

	// Since we released the read-lock in between, it's
	// better to re-check the last-called time
	if time.Since(m.lastCalled) >= m.cacheTime {
		m.cachedValue = m.provider()
		m.lastCalled = time.Now()
	}
	return m.cachedValue
}
