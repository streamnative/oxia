package get

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	keys           []string
	binaryPayloads bool

	ErrorIncorrectBinaryFlagUse = errors.New("binary flag was set when config is being sourced from stdin")
)

func init() {
	Cmd.Flags().StringSliceVarP(&keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().BoolVarP(&binaryPayloads, "binary", "b", false, "Output payloads as a base64 encoded string, use when payloads are binary")
}

var Cmd = &cobra.Command{
	Use:   "get",
	Short: "Get entries",
	Long:  `Get the payloads of the entries associated with the given keys.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	if len(keys) > 0 {
		for _, k := range keys {
			fmt.Printf("Get %v\n", k)
		}
	} else {
		if binaryPayloads {
			return ErrorIncorrectBinaryFlagUse
		}
		fmt.Println("Get - read keys from STDIN")
	}
	return nil
}
