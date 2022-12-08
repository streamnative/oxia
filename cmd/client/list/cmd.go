package list

import (
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

	ErrorExpectedRangeInconsistent = errors.New("inconsistent flags; min and max flags must be in pairs")
)

type flags struct {
	keyMinimums []string
	keyMaximums []string
}

func init() {
	Cmd.Flags().StringSliceVarP(&f.keyMinimums, "key-min", "n", []string{}, "Key range minimum (inclusive)")
	Cmd.Flags().StringSliceVarP(&f.keyMaximums, "key-max", "x", []string{}, "Key range maximum (exclusive)")
	Cmd.MarkFlagsRequiredTogether("key-min", "key-max")
}

var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List keys",
	Long:  `List keys that fall within the given key ranges.`,
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
	if len(flags.keyMinimums) != len(flags.keyMaximums) {
		return ErrorExpectedRangeInconsistent
	}
	if len(flags.keyMinimums) > 0 {
		for i, n := range flags.keyMinimums {
			queries <- Query{
				KeyMinimum: n,
				KeyMaximum: flags.keyMaximums[i],
			}
		}
	} else {
		common.ReadStdin(in, Query{}, queries)
	}
	return nil
}

type Query struct {
	KeyMinimum string `json:"key_minimum"`
	KeyMaximum string `json:"key_maximum"`
}

func (query Query) Perform(client oxia.AsyncClient) common.Call {
	return Call{
		clientCall: client.List(query.KeyMinimum, query.KeyMaximum),
	}
}

func (query Query) Unmarshal(b []byte) (common.Query, error) {
	q := Query{}
	err := json.Unmarshal(b, &q)
	return q, err
}

type Call struct {
	clientCall <-chan oxia.ListResult
}

func (call Call) Complete() any {
	result := <-call.clientCall
	if result.Err != nil {
		return common.OutputError{
			Err: result.Err.Error(),
		}
	} else {
		return Output{
			Keys: result.Keys,
		}
	}
}

type Output struct {
	Keys []string `json:"keys"`
}
