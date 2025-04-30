package channel

import (
	"context"

	"github.com/streamnative/oxia/common/entities"
)

func ReadAll[T any](ctx context.Context, ch chan *entities.TWithError[T]) ([]T, error) {
	container := make([]T, 0)
	for {
		select {
		case t, more := <-ch:
			if !more {
				return container, nil
			}
			if t.Err != nil {
				return nil, t.Err
			}
			container = append(container, t.T)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
