package callback

type streamCallbackChannelAdaptor[T any] struct {
	dataCh chan T
	errCh  chan error
}

func (c *streamCallbackChannelAdaptor[T]) OnNext(t T) error {
	c.dataCh <- t
	return nil
}

func (c *streamCallbackChannelAdaptor[T]) OnComplete(err error) {
	if err != nil {
		c.errCh <- err
	}
	close(c.dataCh)
	close(c.errCh)
}

func ReadFromStreamCallback[T any](dataCh chan T, errCh chan error) StreamCallback[T] {
	adaptor := &streamCallbackChannelAdaptor[T]{dataCh: dataCh, errCh: errCh}
	return NewStreamOnce(adaptor)
}

type streamCallbackCompleteOnly struct{ onComplete func(err error) }

func (c *streamCallbackCompleteOnly) OnNext(any) error     { return nil }
func (c *streamCallbackCompleteOnly) OnComplete(err error) { c.onComplete(err) }
