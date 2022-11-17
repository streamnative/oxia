package batch

type batch interface {
	add(any)
	size() int
	complete()
	fail(error)
}
