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

package get

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Config = flags{}

	ErrorIncorrectBinaryFlagUse = errors.New("binary flag was set when config is being sourced from stdin")
)

type flags struct {
	keys         []string
	binaryValues bool
}

func (flags *flags) Reset() {
	flags.keys = nil
	flags.binaryValues = false
}

func init() {
	Cmd.Flags().StringSliceVarP(&Config.keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().BoolVarP(&Config.binaryValues, "binary", "b", false, "Output values as a base64 encoded string, use when values are binary")
}

var Cmd = &cobra.Command{
	Use:   "get",
	Short: "Get entries",
	Long:  `Get the values of the entries associated with the given keys.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	loop, err := common.NewCommandLoop(cmd.OutOrStdout())
	if err != nil {
		return err
	}
	defer func() {
		loop.Complete()
	}()
	return _exec(Config, cmd.InOrStdin(), loop)
}

func _exec(flags flags, in io.Reader, queue common.QueryQueue) error {
	if len(flags.keys) > 0 {
		for _, k := range flags.keys {
			if flags.binaryValues {
				queue.Add(Query{
					Key:    k,
					Binary: &flags.binaryValues,
				})
			} else {
				queue.Add(Query{
					Key: k,
				})
			}
		}
	} else {
		if flags.binaryValues {
			return ErrorIncorrectBinaryFlagUse
		}
		common.ReadStdin(in, Query{}, queue)
	}
	return nil
}

type Query struct {
	Key    string `json:"key"`
	Binary *bool  `json:"binary,omitempty"`
}

func (query Query) Perform(client oxia.AsyncClient) common.Call {
	call := Call{
		clientCall: client.Get(query.Key),
		binary:     false,
	}
	if query.Binary != nil {
		call.binary = *query.Binary
	}
	return call
}

func (query Query) Unmarshal(b []byte) (common.Query, error) {
	q := Query{}
	err := json.Unmarshal(b, &q)
	return q, err
}

type Call struct {
	binary     bool
	clientCall <-chan oxia.GetResult
}

func (call Call) Complete() <-chan any {
	ch := make(chan any, 1)
	result := <-call.clientCall
	if result.Err != nil {
		ch <- common.OutputError{
			Err: result.Err.Error(),
		}
	} else {
		rawValue := result.Value
		var value string
		if call.binary {
			value = base64.StdEncoding.EncodeToString(rawValue)
		} else {
			value = string(rawValue)
		}
		output := Output{
			Binary: &call.binary,
			Value:  value,
			Version: common.OutputVersion{
				VersionId:          result.Version.VersionId,
				CreatedTimestamp:   result.Version.CreatedTimestamp,
				ModifiedTimestamp:  result.Version.ModifiedTimestamp,
				ModificationsCount: result.Version.ModificationsCount,
			},
		}
		if call.binary {
			output.Binary = &call.binary
		}
		ch <- output
	}
	close(ch)
	return ch
}

type Output struct {
	Binary  *bool                `json:"binary,omitempty"`
	Value   string               `json:"value"`
	Version common.OutputVersion `json:"version"`
}
