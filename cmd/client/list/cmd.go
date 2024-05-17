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

package list

import (
	"context"
	"encoding/json"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Config = flags{}
)

type flags struct {
	keyMin string
	keyMax string
}

func (flags *flags) Reset() {
	flags.keyMin = ""
	flags.keyMin = ""
}

func init() {
	Cmd.Flags().StringVar(&Config.keyMin, "key-min", "", "Key range minimum (inclusive)")
	Cmd.Flags().StringVar(&Config.keyMax, "key-max", "", "Key range maximum (exclusive)")
}

var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List keys",
	Long:  `List keys that fall within the given key range.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, _ []string) error {
	loop, _ := common.NewCommandLoop(cmd.OutOrStdout())
	defer func() {
		loop.Complete()
	}()
	return _exec(Config, loop)
}

func _exec(flags flags, queue common.QueryQueue) error {
	query := Query{
		KeyMinimum: flags.keyMin,
		KeyMaximum: flags.keyMax,
	}
	if flags.keyMax == "" {
		// By default, do not list internal keys
		query.KeyMaximum = "__oxia/"
	}
	queue.Add(query)
	return nil
}

type Query struct {
	KeyMinimum string `json:"key_minimum"`
	KeyMaximum string `json:"key_maximum"`
}

func (query Query) Perform(client oxia.AsyncClient) common.Call {
	return Call{
		clientCall: client.List(context.Background(), query.KeyMinimum, query.KeyMaximum),
	}
}

func (Query) Unmarshal(b []byte) (common.Query, error) {
	q := Query{}
	err := json.Unmarshal(b, &q)
	return q, err
}

type Call struct {
	clientCall <-chan oxia.ListResult
}

func (call Call) Complete() <-chan any {
	ch := make(chan any)
	go func() {
		emptyOutput := true
		for {
			result, ok := <-call.clientCall
			if !ok {
				break
			}

			emptyOutput = false
			if result.Err != nil {
				ch <- common.OutputError{
					Err: result.Err.Error(),
				}
			} else {
				ch <- result.Keys
			}
		}
		if emptyOutput {
			ch <- []string{}
		}
		close(ch)
	}()
	return ch
}
