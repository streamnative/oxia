package get

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/spf13/cobra"
	"io"
	"os"
	"oxia/cmd/client/common"
	"oxia/oxia"
)

var (
	f                           = flags{}
	in      io.Reader           = os.Stdin
	queries chan<- common.Query = common.Queries
	done    <-chan bool         = common.Done

	ErrorIncorrectBinaryFlagUse = errors.New("binary flag was set when config is being sourced from stdin")
)

type flags struct {
	keys           []string
	binaryPayloads bool
}

func init() {
	Cmd.Flags().StringSliceVarP(&f.keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().BoolVarP(&f.binaryPayloads, "binary", "b", false, "Output payloads as a base64 encoded string, use when payloads are binary")
}

var Cmd = &cobra.Command{
	Use:   "get",
	Short: "Get entries",
	Long:  `Get the payloads of the entries associated with the given keys.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	defer func() {
		close(queries)
		<-done
	}()
	return _exec(f, in, queries)
}

func _exec(flags flags, in io.Reader, queries chan<- common.Query) error {
	if len(flags.keys) > 0 {
		for _, k := range flags.keys {
			if flags.binaryPayloads {
				queries <- Query{
					Key:    k,
					Binary: &flags.binaryPayloads,
				}
			} else {
				queries <- Query{
					Key: k,
				}
			}
		}
	} else {
		if flags.binaryPayloads {
			return ErrorIncorrectBinaryFlagUse
		}
		common.ReadStdin(in, Query{}, queries)
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
