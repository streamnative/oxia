package list

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	keyMinimums []string
	keyMaximums []string

	ErrorExpectedRangeInconsistent = errors.New("inconsistent flags; min and max flags must be in pairs")
)

func init() {
	Cmd.Flags().StringSliceVarP(&keyMinimums, "key-min", "n", []string{}, "Key range minimum (inclusive)")
	Cmd.Flags().StringSliceVarP(&keyMaximums, "key-max", "x", []string{}, "Key range maximum (exclusive)")
}

var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List keys",
	Long:  `List keys that fall within the given key ranges.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	if len(keyMinimums) != len(keyMaximums) {
		return ErrorExpectedRangeInconsistent
	}
	if len(keyMinimums) > 0 {
		for i, n := range keyMinimums {
			fmt.Printf("List range %v -> %v\n", n, keyMaximums[i])
		}
	} else {
		fmt.Println("List - read ranges from STDIN")
	}
	return nil
}
