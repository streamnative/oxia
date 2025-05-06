package sequence

import (
	"github.com/spf13/cobra"
	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proofs/workers"
)

var Cmd = &cobra.Command{
	Use:   "sequence",
	Short: "sequence",
	Long:  `run sequence proof worker`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func init() {

}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := oxia.NewSyncClient(common.Config.ServiceAddr, oxia.WithNamespace(common.Config.Namespace), oxia.WithRequestTimeout(common.Config.RequestTimeout))
	if err != nil {
		return err
	}
	proof := workers.NewSequenceProof(client)
	if err = proof.Bootstrap(cmd.Context()); err != nil {
		return err
	}
	return nil
}
