package common

import (
	"os"
	"oxia/oxia"
)

var (
	Queries = make(chan Query, 100)
	Done    = make(chan bool)
)

type Input interface {
	Unmarshal(b []byte) (Query, error)
}
type Query interface {
	Perform(client oxia.AsyncClient) Call
}
type Call interface {
	Complete() any
}

func QueryLoop(queries <-chan Query, client oxia.AsyncClient, out *os.File) {
	defer func(out *os.File) {
		_ = out.Close()
	}(out)
	for {
		query, ok := <-queries
		if !ok {
			break
		}
		writeOutput(out, query.Perform(client).Complete())
	}
	Done <- true
}
