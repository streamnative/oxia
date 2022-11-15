package client

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	pb "google.golang.org/protobuf/proto"
	"oxia/common"
	"oxia/proto"
	"time"
)

var (
	// Used for flags.
	serviceAddr string

	Cmd = &cobra.Command{
		Use:   "client",
		Short: "Short description",
		Long:  `Test client`,
		Run:   main,
	}
)

func init() {
	Cmd.Flags().StringVarP(&serviceAddr, "service-address", "a", "localhost:9190", "Service address")
}

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	clientPool := common.NewClientPool()
	defer clientPool.Close()

	// Set up a connection to the server.
	c, err := clientPool.GetClientRpc(serviceAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect")
	}

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	gsaClient, err := c.ShardAssignments(ctx, &proto.ShardAssignmentsRequest{})
	if err != nil {
		log.Fatal().Err(err).Msg("ShardAssignments failed")
	}

	sa, err := gsaClient.Recv()
	if err != nil {
		log.Fatal().Err(err).Msg("ShardAssignments failed")
	}

	log.Info().
		Interface("assignment", sa).
		Msg("Received assignments")

	if res, err := c.Write(ctx, &proto.WriteRequest{
		ShardId: pb.Uint32(0),
		Puts: []*proto.PutRequest{{
			Key:             "a",
			Payload:         []byte("Hello world"),
			ExpectedVersion: nil,
		}},
	}); err != nil {
		log.Fatal().Err(err).Msg("Put operation failed")
	} else {
		log.Info().
			Interface("status", res.Puts[0].Status).
			Interface("stat", res.Puts[0].Stat).
			Msg("Put operation completed")
	}

	// Try to read it back

	if res, err := c.Read(ctx, &proto.ReadRequest{
		ShardId: pb.Uint32(0),
		Gets: []*proto.GetRequest{{
			Key:            "a",
			IncludePayload: true,
		}},
	}); err != nil {
		log.Fatal().Err(err).Msg("Put operation failed")
	} else {
		log.Info().
			Interface("status", res.Gets[0].Status).
			Interface("stat", res.Gets[0].Stat).
			Str("value", string(res.Gets[0].Payload)).
			Msg("Get operation completed")
	}
}
