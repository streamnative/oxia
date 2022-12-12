package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"
	"os"
	"oxia/cmd/client"
	"oxia/cmd/controller"
	"oxia/cmd/coordinator"
	"oxia/cmd/operator"
	"oxia/cmd/server"
	"oxia/cmd/standalone"
	"oxia/common"
)

var (
	rootCmd = &cobra.Command{
		Use:   "oxia",
		Short: "Short description",
		Long:  `Long description`,
	}
)

func init() {
	rootCmd.PersistentFlags().BoolVarP(&common.LogDebug, "log-debug", "d", false, "Enable debug logs")
	rootCmd.PersistentFlags().BoolVarP(&common.LogJson, "log-json", "j", false, "Print logs in JSON format")
	rootCmd.PersistentFlags().BoolVar(&common.PprofEnable, "profile", false, "Enable pprof profiler")
	rootCmd.PersistentFlags().StringVar(&common.PprofBindAddress, "profile-bind-address", "127.0.0.1:6060", "Bind address for pprof")

	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(controller.Cmd)
	rootCmd.AddCommand(coordinator.Cmd)
	rootCmd.AddCommand(operator.Cmd)
	rootCmd.AddCommand(server.Cmd)
	rootCmd.AddCommand(standalone.Cmd)
}

func main() {
	common.DoWithLabels(map[string]string{
		"oxia": "main",
	}, func() {
		if _, err := maxprocs.Set(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := rootCmd.Execute(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	})
}
