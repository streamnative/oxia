// Copyright 2024 StreamNative, Inc.
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
	"bytes"
	"github.com/stretchr/testify/assert"
	"oxia/oxia"
	"testing"
)

func TestWriteOutput(t *testing.T) {
	for _, test := range []struct {
		name     string
		result   any
		expected string
	}{
		{"common.OutputError", OutputError{Err: "hello"}, "{\"error\":\"hello\"}\n"},
		{"common.OutputErrorEmpty", OutputError{}, "{}\n"},
		{"common.OutputVersion", OutputVersion{
			VersionId:          1,
			CreatedTimestamp:   2,
			ModifiedTimestamp:  3,
			ModificationsCount: 1,
		}, "{\"version_id\":1,\"created_timestamp\":2,\"modified_timestamp\":3,\"modifications_count\":1}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := bytes.NewBufferString("")
			writeOutput(b, test.result)
			assert.Equal(t, test.expected, b.String())
		})
	}
}

func TestReadStdin(t *testing.T) {
	for _, test := range []struct {
		name     string
		stdin    string
		inputs   []string
		expected []Query
	}{
		{"one", "a", []string{"a"}, []Query{&fakeQuery{"a"}}},
		{"two", "a\nb\n", []string{"a", "b"}, []Query{&fakeQuery{"a"}, &fakeQuery{"b"}}},
		{"two-no-cr", "a\nb", []string{"a", "b"}, []Query{&fakeQuery{"a"}, &fakeQuery{"b"}}},
		{"none", "", []string{}, []Query{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := fakeQueryQueue{[]Query{}}
			in := bytes.NewBufferString(test.stdin)
			m := make(map[string]Query, len(test.inputs))
			for i, k := range test.inputs {
				m[k] = test.expected[i]
			}
			input := fakeInput{m}
			ReadStdin(in, &input, &queue)
			assert.Equal(t, test.expected, queue.queries)
		})
	}
}

type fakeInput struct {
	pairs map[string]Query
}

func (i *fakeInput) Unmarshal(b []byte) (Query, error) {
	return i.pairs[string(b)], nil
}

type fakeQuery struct {
	id string
}

func (i *fakeQuery) Perform(client oxia.AsyncClient) Call {
	return nil
}

type fakeQueryQueue struct {
	queries []Query
}

func (q *fakeQueryQueue) Add(query Query) {
	if q.queries == nil {
		q.queries = []Query{}
	}
	q.queries = append(q.queries, query)
}
