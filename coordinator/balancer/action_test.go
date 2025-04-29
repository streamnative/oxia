package balancer

import (
	"sync"
	"testing"
)

func TestActionSwapDone(t *testing.T) {
	group := &sync.WaitGroup{}
	group.Add(1)
	swapAction := SwapNodeAction{
		Shard:  int64(1),
		From:   "sv-1",
		To:     "sv-2",
		waiter: group,
	}
	swapAction.Done()
	group.Wait()
}
