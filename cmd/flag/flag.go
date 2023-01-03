package flag

import (
	"github.com/spf13/cobra"
	"oxia/kubernetes"
)

func PublicPort(cmd *cobra.Command, conf *int) {
	cmd.Flags().IntVarP(conf, "public-port", "p", kubernetes.PublicPort.Port, "Public service port")
}

func InternalPort(cmd *cobra.Command, conf *int) {
	cmd.Flags().IntVarP(conf, "internal-port", "i", kubernetes.InternalPort.Port, "Internal service port")
}

func MetricsPort(cmd *cobra.Command, conf *int) {
	cmd.Flags().IntVarP(conf, "metrics-port", "m", kubernetes.MetricsPort.Port, "Metrics port")
}
