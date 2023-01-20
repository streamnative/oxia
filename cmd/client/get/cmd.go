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
	"github.com/spf13/cobra"
	"io"
	"oxia/cmd/client/common"
	"oxia/oxia"
)

var (
	Config = flags{}

	ErrorIncorrectBinaryFlagUse = errors.New("binary flag was set when config is being sourced from stdin")
)

type flags struct {
	keys           []string
	binaryPayloads bool
}

func (flags *flags) Reset() {
	flags.keys = nil
	flags.binaryPayloads = false
}

func init() {
	Cmd.Flags().StringSliceVarP(&Config.keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().BoolVarP(&Config.binaryPayloads, "binary", "b", false, "Output payloads as a base64 encoded string, use when payloads are binary")
}

var Cmd = &cobra.Command{
	Use:   "get",
	Short: "Get entries",
	Long:  `Get the payloads of the entries associated with the given keys.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	loop, _ := common.NewCommandLoop(cmd.OutOrStdout())
	defer func() {
		loop.Complete()
	}()
	return _exec(Config, cmd.InOrStdin(), loop)
}

func _exec(flags flags, in io.Reader, queue common.QueryQueue) error {
	if len(flags.keys) > 0 {
		for _, k := range flags.keys {
			if flags.binaryPayloads {
				queue.Add(Query{
					Key:    k,
					Binary: &flags.binaryPayloads,
				})
			} else {
				queue.Add(Query{
					Key: k,
				})
			}
		}
	} else {
		if flags.binaryPayloads {
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

func (call Call) Complete() any {
	result := <-call.clientCall
	if result.Err != nil {
		return common.OutputError{
			Err: result.Err.Error(),
		}
	} else {
		rawPayload := result.Payload
		var payload string
		if call.binary {
			payload = base64.StdEncoding.EncodeToString(rawPayload)
		} else {
			payload = string(rawPayload)
		}
		output := Output{
			Binary:  &call.binary,
			Payload: payload,
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
		return output
	}
}

type Output struct {
	Binary  *bool                `json:"binary,omitempty"`
	Payload string               `json:"payload"`
	Version common.OutputVersion `json:"version"`
}
