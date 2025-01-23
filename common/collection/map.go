package collection

type Map[K comparable, V any] interface {
	Put(key K, value V)
	Get(key K) (value V, found bool)
	Remove(key K)
	Keys() []K
	Empty() bool
	Size() int
	Clear()
	Values() []V
	String() string
}

func NewVisibleMap[K comparable, V any]() Map[K, V] {
	return &visibleMap[K, V]{
		container: make(map[K]V),
	}
}
