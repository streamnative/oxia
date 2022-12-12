package main

import (
	"fmt"
	"github.com/rs/zerolog"
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
	logLevelStr string
	rootCmd     = &cobra.Command{
		Use:     "oxia",
		Short:   "Short description",
		Long:    `Long description`,
		PreRunE: configureLogLevel,
	}
)

type LogLevelError string

func (l LogLevelError) Error() string {
	return fmt.Sprintf("unknown log level (%s)", string(l))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", common.DefaultLogLevel.String(), "Set logging level [disabled|trace|debug|info|warn|error|fatal|panic]")
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

func configureLogLevel(cmd *cobra.Command, args []string) error {
	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	common.LogLevel = logLevel
	return nil
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
