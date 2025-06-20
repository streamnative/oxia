package action

import "sync"

type ElectionAction struct {
	Shard int64

	NewLeader string
	Waiter    *sync.WaitGroup
}

func (e *ElectionAction) Done(leader any) {
	e.NewLeader = leader.(string)
	e.Waiter.Done()
}

func (*ElectionAction) Type() Type {
	return Election
}

func (e *ElectionAction) Clone() *ElectionAction {
	return &ElectionAction{
		Shard:  e.Shard,
		Waiter: &sync.WaitGroup{},
	}
}
