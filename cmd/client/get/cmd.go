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
			Stat: common.OutputStat{
				Version:           result.Stat.Version,
				CreatedTimestamp:  result.Stat.CreatedTimestamp,
				ModifiedTimestamp: result.Stat.ModifiedTimestamp,
			},
		}
		if call.binary {
			output.Binary = &call.binary
		}
		return output
	}
}

type Output struct {
	Binary  *bool             `json:"binary,omitempty"`
	Payload string            `json:"payload"`
	Stat    common.OutputStat `json:"stat"`
}
