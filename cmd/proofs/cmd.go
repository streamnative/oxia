package proofs

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/cmd/proofs/sequence"
	oxiacommon "github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Cmd = &cobra.Command{
		Use:   "proofs",
		Short: "Proof of correctness",
		Long:  `Proof of correctness for the oxia cluster.`,
	}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", oxiacommon.DefaultPublicPort)
	Cmd.PersistentFlags().StringVarP(&common.Config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.PersistentFlags().StringVarP(&common.Config.Namespace, "namespace", "n", oxia.DefaultNamespace, "The Oxia namespace to use")
	Cmd.PersistentFlags().DurationVar(&common.Config.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Requests timeout")

	Cmd.AddCommand(sequence.Cmd)
}
