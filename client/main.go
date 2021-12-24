package client

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
		Use:   "client",
		Short: "Short description",
		Long:  `Test client`,
		Run:   main,
	}
)

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	clientPool := common.NewClientPool()
	defer clientPool.Close()

	// Set up a connection to the server.
	c, err := clientPool.GetClientRpc("localhost:9190")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect")
	}

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	gsaClient, err := c.GetShardsAssignments(ctx, &proto.Empty{})
	if err != nil {
		log.Fatal().Err(err).Msg("GetShardsAssignments failed")
	}

	sa, err := gsaClient.Recv()
	if err != nil {
		log.Fatal().Err(err).Msg("GetShardsAssignments failed")
	}

	log.Info().
		Interface("assignment", sa).
		Msg("Received assignments")

	r, err := c.Put(ctx, &proto.PutOp{})

	if err != nil {
		log.Error().Err(err).Msg("Put operation failed")
	} else {
		log.Info().
			Interface("res", r).
			Msg("Operation succeeded")
	}
}
