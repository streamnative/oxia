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

package put

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

	ErrorExpectedKeyPayloadInconsistent = errors.New("inconsistent flags; key and payload flags must be in pairs")
	ErrorExpectedVersionInconsistent    = errors.New("inconsistent flags; zero or all keys must have an expected version")
	ErrorBase64PayloadInvalid           = errors.New("binary flag was set but payload is not valid base64")
	ErrorIncorrectBinaryFlagUse         = errors.New("binary flag was set when config is being sourced from stdin")
)

type flags struct {
	keys             []string
	payloads         []string
	expectedVersions []int64
	binaryPayloads   bool
}

func (flags *flags) Reset() {
	flags.keys = nil
	flags.payloads = nil
	flags.expectedVersions = nil
	flags.binaryPayloads = false
}

func init() {
	Cmd.Flags().StringSliceVarP(&Config.keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().StringSliceVarP(&Config.payloads, "payload", "p", []string{}, "Associated payload, assumed to be encoded with local charset unless -b is used")
	Cmd.Flags().Int64SliceVarP(&Config.expectedVersions, "expected-version", "e", []int64{}, "Version of entry expected to be on the server")
	Cmd.Flags().BoolVarP(&Config.binaryPayloads, "binary", "b", false, "Base64 decode the input payloads (required for binary payloads)")
	Cmd.MarkFlagsRequiredTogether("key", "payload")
}

var Cmd = &cobra.Command{
	Use:   "put",
	Short: "Put payloads",
	Long:  `Put a payloads and associated them with the given keys, either inserting a new entries or updating existing ones. If an expected version is provided, the put will only take place if it matches the version of the current record on the server`,
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
	if len(flags.keys) != len(flags.payloads) && (len(flags.payloads) > 0 || len(flags.keys) > 0) {
		return ErrorExpectedKeyPayloadInconsistent
	}
	if (len(flags.expectedVersions) > 0) && len(flags.keys) != len(flags.expectedVersions) {
		return ErrorExpectedVersionInconsistent
	}
	if len(flags.keys) > 0 {
		for i, k := range flags.keys {
			query := Query{
				Key:     k,
				Payload: flags.payloads[i],
				Binary:  &flags.binaryPayloads,
			}
			if len(flags.expectedVersions) > 0 {
				query.ExpectedVersion = &flags.expectedVersions[i]
			}
			queue.Add(query)
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
	Key             string `json:"key"`
	Payload         string `json:"payload"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
	Binary          *bool  `json:"binary,omitempty"`
}

func (query Query) Perform(client oxia.AsyncClient) common.Call {
	binary := false
	if query.Binary != nil {
		binary = *query.Binary
	}
	payload, err := convertPayload(binary, query.Payload)
	call := Call{}
	if err != nil {
		errChan := make(chan oxia.PutResult, 1)
		errChan <- oxia.PutResult{Err: err}
		call.clientCall = errChan
	} else {
		var putOptions []oxia.PutOption
		if query.ExpectedVersion != nil {
			putOptions = append(putOptions, oxia.ExpectedVersion(*query.ExpectedVersion))
		}
		call.clientCall = client.Put(query.Key, payload, putOptions...)
	}
	return call
}

func (query Query) Unmarshal(b []byte) (common.Query, error) {
	q := Query{}
	err := json.Unmarshal(b, &q)
	return q, err
}

func convertPayload(binary bool, payload string) ([]byte, error) {
	if binary {
		decoded := make([]byte, int64(float64(len(payload))*0.8))
		_, err := base64.StdEncoding.Decode(decoded, []byte(payload))
		if err != nil {
			return nil, ErrorBase64PayloadInvalid
		}
		return decoded, nil
	} else {
		return []byte(payload), nil
	}
}

type Call struct {
	clientCall <-chan oxia.PutResult
}

func (call Call) Complete() any {
	result := <-call.clientCall
	if result.Err != nil {
		return common.OutputError{
			Err: result.Err.Error(),
		}
	} else {
		return Output{
			Stat: common.OutputStat{
				Version:           result.Stat.Version,
				CreatedTimestamp:  result.Stat.CreatedTimestamp,
				ModifiedTimestamp: result.Stat.ModifiedTimestamp,
			},
		}
	}
}

type Output struct {
	Stat common.OutputStat `json:"stat"`
}
