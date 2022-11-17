package batch

type Batch interface {
	Add(any)
	Size() int
	Complete()
	Fail(error)
}
