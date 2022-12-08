package common

import (
	"bufio"
	"encoding/json"
	"io"
)

func ReadStdin(stdin io.Reader, input Input, queue QueryQueue) {
	scanner := bufio.NewScanner(stdin)
	for {
		scanner.Scan()
		b := scanner.Bytes()
		if len(b) != 0 {
			query, err := input.Unmarshal(b)
			if err != nil {
				panic(err)
			}
			queue.Add(query)
		} else {
			break
		}
	}
}

func writeOutput(out io.Writer, value any) {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	_, err = out.Write(append(b, "\n"...))
	if err != nil {
		panic(err)
	}
}
