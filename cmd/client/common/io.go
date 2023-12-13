// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		if len(b) == 0 {
			break
		}

		query, err := input.Unmarshal(b)
		if err != nil {
			panic(err)
		}
		queue.Add(query)
	}
}

func writeOutputCh(out io.Writer, valuesCh <-chan any) {
	for value := range valuesCh {
		writeOutput(out, value)
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
