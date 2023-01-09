package common

import "context"

// WaitGroup is similar to sync.WaitGroup but adds 2 capabilities:
//  1. Returning an error if any operation fails
//  2. Accept a context to cancel the Wait
type WaitGroup interface {

	// Wait until all the parties in the group are either done or if there is any failure
	// You should only call wait once
	Wait(ctx context.Context) error

	// Done Signals that one party in the group is done
	Done()

	// Fail Signal that one party has failed in the operation
	Fail(err error)
}

type waitGroup struct {
	parties   int
	responses chan error
}

func NewWaitGroup(parties int) WaitGroup {
	return &waitGroup{
		parties:   parties,
		responses: make(chan error, parties),
	}
}

func (g *waitGroup) Wait(ctx context.Context) error {
	for i := 0; i < g.parties; i++ {
		select {
		case err := <-g.responses:
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Everyone has completed successfully
	return nil
}

func (g *waitGroup) Done() {
	g.responses <- nil
}

func (g *waitGroup) Fail(err error) {
	g.responses <- err
}
