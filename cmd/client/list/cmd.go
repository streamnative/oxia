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
	"errors"
	"io"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Config = flags{}

	ErrExpectedRangeInconsistent = errors.New("inconsistent flags; min and max flags must be in pairs")
)

type flags struct {
	keyMinimums []string
	keyMaximums []string
}

func (flags *flags) Reset() {
	flags.keyMinimums = nil
	flags.keyMaximums = nil
}

func init() {
	Cmd.Flags().StringSliceVar(&Config.keyMinimums, "key-min", []string{}, "Key range minimum (inclusive)")
	Cmd.Flags().StringSliceVar(&Config.keyMaximums, "key-max", []string{}, "Key range maximum (exclusive)")
	Cmd.MarkFlagsRequiredTogether("key-min", "key-max")
}

var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List keys",
	Long:  `List keys that fall within the given key ranges.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, _ []string) error {
	loop, _ := common.NewCommandLoop(cmd.OutOrStdout())
	defer func() {
		loop.Complete()
	}()
	return _exec(Config, cmd.InOrStdin(), loop)
}

func _exec(flags flags, in io.Reader, queue common.QueryQueue) error {
	if len(flags.keyMinimums) != len(flags.keyMaximums) {
		return ErrExpectedRangeInconsistent
	}
	if len(flags.keyMinimums) > 0 {
		for i, n := range flags.keyMinimums {
			queue.Add(Query{
				KeyMinimum: n,
				KeyMaximum: flags.keyMaximums[i],
			})
		}
	} else {
		common.ReadStdin(in, Query{}, queue)
	}
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
				ch <- Output{
					Keys: result.Keys,
				}
			}
		}
		if emptyOutput {
			ch <- Output{Keys: make([]string, 0)}
		}
		close(ch)
	}()
	return ch
}

type Output struct {
	Keys []string `json:"keys"`
}
