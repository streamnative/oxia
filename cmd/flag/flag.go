package flag

import (
	"github.com/spf13/cobra"
	"oxia/operator/resource"
)

func PublicPort(cmd *cobra.Command, conf *int) {
	cmd.Flags().IntVarP(conf, "public-port", "p", resource.PublicPort(), "Public service port")
}

func InternalPort(cmd *cobra.Command, conf *int) {
	cmd.Flags().IntVarP(conf, "internal-port", "i", resource.InternalPort(), "Internal service port")
}

func MetricsPort(cmd *cobra.Command, conf *int) {
	cmd.Flags().IntVarP(conf, "metrics-port", "m", resource.MetricsPort(), "Metrics port")
}
