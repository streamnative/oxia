package balancer

import (
	"sync"
)

type ActionType string

const (
	SwapNode ActionType = "swap-node"
)

type Action interface {
	Type() ActionType
}

var _ Action = &SwapNodeAction{}

type SwapNodeAction struct {
	Shard int64
	From  string
	To    string

	waiter *sync.WaitGroup
}

func (s *SwapNodeAction) Type() ActionType {
	return SwapNode
}
