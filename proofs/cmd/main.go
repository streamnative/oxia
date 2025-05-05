package main

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/oxia"
	proofcommon "github.com/streamnative/oxia/proofs/cmd/common"
	"github.com/streamnative/oxia/proofs/cmd/sequence"
	"go.uber.org/automaxprocs/maxprocs"
)

var (
	rootCmd = &cobra.Command{
		Use:   "oxia-proof",
		Short: "oxia-proof root command",
		Long:  `oxia-proof root command`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logLevel, err := common.ParseLogLevel(proofcommon.ProofPv.LogLevelStr)
			if err != nil {
				return err
			}
			common.LogLevel = logLevel
			common.ConfigureLogger()
			return nil
		},
		SilenceUsage: true,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&proofcommon.ProofPv.LogLevelStr, "log-level", "l", common.DefaultLogLevel.String(), "Set logging level [debug|info|warn|error]")
	rootCmd.PersistentFlags().BoolVar(&proofcommon.ProofPv.PprofEnable, "profile", false, "Enable pprof profiler")
	rootCmd.PersistentFlags().StringVar(&proofcommon.ProofPv.PprofBindAddress, "profile-bind-address", "127.0.0.1:6060", "Bind address for pprof")
	rootCmd.PersistentFlags().StringVarP(&proofcommon.ProofPv.ServiceAddress, "service-address", "a", "127.0.0.1:6648", "Service address")
	rootCmd.PersistentFlags().StringVarP(&proofcommon.ProofPv.Namespace, "namespace", "n", oxia.DefaultNamespace, "The Oxia namespace to use")
	rootCmd.PersistentFlags().DurationVar(&proofcommon.ProofPv.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Requests timeout")

	rootCmd.AddCommand(sequence.Cmd)
}

func main() {
	common.DoWithLabels(
		context.Background(),
		map[string]string{
			"component": "oxia-proof-main",
		},
		func() {
			if _, err := maxprocs.Set(); err != nil {
				panic(err)
			}
			if err := rootCmd.Execute(); err != nil {
				panic(err)
			}
		},
	)
}
