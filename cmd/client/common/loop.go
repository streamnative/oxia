package common

import (
	"io"
	"oxia/oxia"
)

type CommandLoop struct {
	queries chan Query
	done    chan bool
	client  oxia.AsyncClient
	out     io.Writer
}

type QueryQueue interface {
	Add(query Query)
}

type Input interface {
	Unmarshal(b []byte) (Query, error)
}
type Query interface {
	Perform(client oxia.AsyncClient) Call
}
type Call interface {
	Complete() any
}

func NewCommandLoop(out io.Writer) (*CommandLoop, error) {
	client, err := Config.NewClient()
	if err != nil {
		return nil, err
	}
	return newCommandLoop(client, out)
}

func newCommandLoop(client oxia.AsyncClient, out io.Writer) (*CommandLoop, error) {
	loop := CommandLoop{
		queries: make(chan Query, 100),
		done:    make(chan bool),
		client:  client,
		out:     out,
	}
	go loop.start()
	return &loop, nil
}

func (loop *CommandLoop) start() {
	for {
		query, ok := <-loop.queries
		if !ok {
			break
		}
		writeOutput(loop.out, query.Perform(loop.client).Complete())
	}
	loop.done <- true
}

func (loop *CommandLoop) Add(query Query) {
	loop.queries <- query
}

func (loop *CommandLoop) Complete() {
	close(loop.queries)
	<-loop.done
}
