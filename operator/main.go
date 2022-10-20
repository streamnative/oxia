package operator

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"oxia/common"
	"oxia/proto"
	"time"
)

var (
	// Used for flags.
	shards            uint32
	staticNodes       []string
	replicationFactor uint32

	Cmd = &cobra.Command{
		Use:   "operator",
		Short: "Start an operator process",
		Long:  `Long description`,
		Run:   main,
	}
)

func init() {
	Cmd.Flags().Uint32VarP(&shards, "shards", "s", 1, "Number of shards")
	Cmd.Flags().StringArrayVarP(&staticNodes, "static-nodes", "n", nil, "Static list of nodes")
	Cmd.MarkFlagRequired("static-nodes")
	Cmd.Flags().Uint32VarP(&replicationFactor, "replication-factor", "r", 1, "The replication factor")
}

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	log.Info().
		Strs("nodes", staticNodes).
		Uint32("shards", shards).
		Uint32("replication-factor", replicationFactor).
		Msg("Starting operator")

	addrs := make([]*proto.ServerAddress, len(staticNodes))
	for i := 0; i < len(staticNodes); i++ {
		addrs[i] = &proto.ServerAddress{
			InternalUrl: staticNodes[i],
			PublicUrl:   staticNodes[i],
		}
	}
	_ = ComputeAssignments(addrs, replicationFactor, shards)

	clientPool := common.NewClientPool()
	defer clientPool.Close()

	// Set up a connection to the server.
	_, err := clientPool.GetInternalRpc("localhost:8190")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect")
	}

	// Contact the server and print out its response.
	_, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	//	r, err := c.UpdateStatus(ctx, cs)

	if err != nil {
		log.Error().Err(err).Msg("Could not update the cluster status")
	} else {
		log.Info().
			Interface("res", nil).
			Msg("Updated cluster status")
	}
}
