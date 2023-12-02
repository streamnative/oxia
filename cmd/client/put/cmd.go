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
	"io"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Config = flags{}

	ErrExpectedKeyValueInconsistent = errors.New("inconsistent flags; key and value flags must be in pairs")
	ErrExpectedVersionInconsistent  = errors.New("inconsistent flags; zero or all keys must have an expected version")
	ErrBase64ValueInvalid           = errors.New("binary flag was set but value is not valid base64")
	ErrIncorrectBinaryFlagUse       = errors.New("binary flag was set when config is being sourced from stdin")
)

type flags struct {
	keys             []string
	values           []string
	expectedVersions []int64
	binaryValues     bool
}

func (flags *flags) Reset() {
	flags.keys = nil
	flags.values = nil
	flags.expectedVersions = nil
	flags.binaryValues = false
}

func init() {
	Cmd.Flags().StringSliceVarP(&Config.keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().StringSliceVarP(&Config.values, "value", "v", []string{}, "Associated value, assumed to be encoded with local charset unless -b is used")
	Cmd.Flags().Int64SliceVarP(&Config.expectedVersions, "expected-version", "e", []int64{}, "Version of entry expected to be on the server")
	Cmd.Flags().BoolVarP(&Config.binaryValues, "binary", "b", false, "Base64 decode the input values (required for binary values)")
	Cmd.MarkFlagsRequiredTogether("key", "value")
}

var Cmd = &cobra.Command{
	Use:   "put",
	Short: "Put values",
	Long:  `Put a values and associated them with the given keys, either inserting a new entries or updating existing ones. If an expected version is provided, the put will only take place if it matches the version of the current record on the server`,
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
	if len(flags.keys) != len(flags.values) && (len(flags.values) > 0 || len(flags.keys) > 0) {
		return ErrExpectedKeyValueInconsistent
	}
	if (len(flags.expectedVersions) > 0) && len(flags.keys) != len(flags.expectedVersions) {
		return ErrExpectedVersionInconsistent
	}
	if len(flags.keys) > 0 {
		for i, k := range flags.keys {
			query := Query{
				Key:    k,
				Value:  flags.values[i],
				Binary: &flags.binaryValues,
			}
			if len(flags.expectedVersions) > 0 {
				query.ExpectedVersion = &flags.expectedVersions[i]
			}
			queue.Add(query)
		}
	} else {
		if flags.binaryValues {
			return ErrIncorrectBinaryFlagUse
		}
		common.ReadStdin(in, Query{}, queue)
	}
	return nil
}

type Query struct {
	Key             string `json:"key"`
	Value           string `json:"value"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
	Binary          *bool  `json:"binary,omitempty"`
}

func (query Query) Perform(client oxia.AsyncClient) common.Call {
	binary := false
	if query.Binary != nil {
		binary = *query.Binary
	}
	value, err := convertValue(binary, query.Value)
	call := Call{}
	if err != nil {
		errChan := make(chan oxia.PutResult, 1)
		errChan <- oxia.PutResult{Err: err}
		call.clientCall = errChan
	} else {
		var putOptions []oxia.PutOption
		if query.ExpectedVersion != nil {
			putOptions = append(putOptions, oxia.ExpectedVersionId(*query.ExpectedVersion))
		}
		call.clientCall = client.Put(query.Key, value, putOptions...)
	}
	return call
}

func (query Query) Unmarshal(b []byte) (common.Query, error) {
	q := Query{}
	err := json.Unmarshal(b, &q)
	return q, err
}

func convertValue(binary bool, value string) ([]byte, error) {
	if binary {
		decoded := make([]byte, int64(float64(len(value))*0.8))
		_, err := base64.StdEncoding.Decode(decoded, []byte(value))
		if err != nil {
			return nil, ErrBase64ValueInvalid
		}
		return decoded, nil
	} else {
		return []byte(value), nil
	}
}

type Call struct {
	clientCall <-chan oxia.PutResult
}

func (call Call) Complete() <-chan any {
	ch := make(chan any, 1)
	result := <-call.clientCall
	if result.Err != nil {
		ch <- common.OutputError{
			Err: result.Err.Error(),
		}
	} else {
		ch <- Output{
			Version: common.OutputVersion{
				VersionId:          result.Version.VersionId,
				CreatedTimestamp:   result.Version.CreatedTimestamp,
				ModifiedTimestamp:  result.Version.ModifiedTimestamp,
				ModificationsCount: result.Version.ModificationsCount,
			},
		}
	}
	close(ch)
	return ch
}

type Output struct {
	Version common.OutputVersion `json:"version"`
}
