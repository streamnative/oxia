package action

import (
	"sync"

	"github.com/oxia-db/oxia/coordinator/model"
)

var _ Action = &SwapNodeAction{}

type SwapNodeAction struct {
	Shard int64
	From  model.Server
	To    model.Server

	Waiter *sync.WaitGroup
}

func (s *SwapNodeAction) Done(_ any) {
	s.Waiter.Done()
}

func (*SwapNodeAction) Type() Type {
	return SwapNode
}
