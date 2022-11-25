package delete

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	keys             []string
	expectedVersions []int64
	keyMinimums      []string
	keyMaximums      []string

	ErrorExpectedVersionInconsistent = errors.New("inconsistent flags; zero or all keys must have an expected version")
	ErrorExpectedRangeInconsistent   = errors.New("inconsistent flags; min and max flags must be in pairs")
)

func init() {
	Cmd.Flags().StringSliceVarP(&keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().Int64SliceVarP(&expectedVersions, "expected-version", "e", []int64{}, "Version of entry expected to be on the server")
	Cmd.Flags().StringSliceVarP(&keyMinimums, "key-min", "n", []string{}, "Key range minimum (inclusive)")
	Cmd.Flags().StringSliceVarP(&keyMaximums, "key-max", "x", []string{}, "Key range maximum (exclusive)")
}

var Cmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete the entries",
	Long:  `Delete the entries with the given keys or key ranges, if they exists. If an expected version is provided, the delete will only take place if it matches the version of the current record on the server`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	if len(keyMinimums) != len(keyMaximums) {
		return ErrorExpectedRangeInconsistent
	}
	if len(keys) != len(expectedVersions) && len(expectedVersions) > 0 {
		return ErrorExpectedVersionInconsistent
	}
	if len(keyMinimums) > 0 || len(keys) > 0 {
		for i, n := range keyMinimums {
			fmt.Printf("Delete range %v -> %v\n", n, keyMaximums[i])
		}
		for i, k := range keys {
			if len(expectedVersions) > 0 {
				fmt.Printf("Delete key %v with expected version %v\n", k, expectedVersions[i])
			} else {
				fmt.Printf("Delete key %v\n", k)
			}
		}
	} else {
		fmt.Println("Delete - read keys/ranges from STDIN")
	}
	return nil
}
