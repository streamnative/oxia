package channel

import "context"

func ReadAll[T any](ch chan T, errCh chan error) ([]T, error) {
	ctx := context.Background()
	container := make([]T, 0)
	var err error
loop:
	for {
		select {
		case data, more := <-ch:
			if !more {
				break loop
			}
			container = append(container, data)
		case err = <-errCh:
			break loop
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return container, err
}
