package client

import (
	"github.com/spf13/cobra"
	"oxia/common"
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

	//// Set up a connection to the server.
	//c, err := clientPool.GetClientRpc(serviceAddr)
	//if err != nil {
	//	log.Fatal().Err(err).Msg("Failed to connect")
	//}
	//
	//// Contact the server and print out its response.
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()

	//gsaClient, err := c.GetShardsAssignments(ctx, &proto.Empty{})
	//if err != nil {
	//	log.Fatal().Err(err).Msg("GetShardsAssignments failed")
	//}
	//
	//sa, err := gsaClient.Recv()
	//if err != nil {
	//	log.Fatal().Err(err).Msg("GetShardsAssignments failed")
	//}
	//
	//log.Info().
	//	Interface("assignment", sa).
	//	Msg("Received assignments")
	//
	//r, err := c.Put(ctx, &proto.PutOp{
	//	ShardId:         0,
	//	Key:             "my-key",
	//	Payload:         []byte("hello"),
	//	ExpectedVersion: pb.Uint64(1),
	//})
	//
	//if err != nil {
	//	log.Error().Err(err).Msg("Put operation failed")
	//} else {
	//	log.Info().
	//		Interface("res", r).
	//		Msg("Operation succeeded")
	//}
}
