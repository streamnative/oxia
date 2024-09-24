package wal

import (
	"github.com/spf13/cobra"
	"github.com/streamnative/oxia/cmd/wal/truncate"
)

var (
	Cmd = &cobra.Command{
		Use:   "wal",
		Short: "Wal utils",
		Long:  `Tools for the oxia WAL`,
	}
)

func init() {
	Cmd.AddCommand(truncate.Cmd)
}
