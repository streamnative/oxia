package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"oxia/client"
	"oxia/common"
	"oxia/operator"
	"oxia/server"
	"oxia/standalone"
)

var (
	// Used for flags.
	//shards            uint32
	//staticNodes       []string
	//replicationFactor uint32

	rootCmd = &cobra.Command{
		Use:   "oxia",
		Short: "Short description",
		Long:  `Long description`,
	}
)

func init() {
	//rootCmd.PersistentFlags().StringVarP(&userLicense, "license", "l", "", "name of license for the project")
	rootCmd.PersistentFlags().BoolVarP(&common.LogDebug, "log-debug", "d", false, "Enable debug logs")
	rootCmd.PersistentFlags().BoolVarP(&common.LogJson, "log-json", "j", false, "Print logs in JSON format")

	rootCmd.AddCommand(server.Cmd)
	rootCmd.AddCommand(operator.Cmd)
	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(standalone.Cmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
