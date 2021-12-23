package cmd

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"os"
	"oxia/common"
	"oxia/proto"
	"time"
)

var (
	// Used for flags.
	logDebug bool
	logJson  bool

	shards            uint32
	staticNodes       []string
	replicationFactor uint32

	rootCmd = &cobra.Command{
		Use:   "oxia-operator",
		Short: "Short description",
		Long:  `Long description`,

		Version: "1.0",

		Run: func(cmd *cobra.Command, args []string) {
			common.ConfigureLogger(logDebug, logJson)

			log.Info().
				Strs("nodes", staticNodes).
				Uint32("shards", shards).
				Uint32("replication-factor", replicationFactor).
				Msg("Starting operator")

			cs := computeAssignments(staticNodes, replicationFactor, shards)

			connectionPool := common.NewConnectionPool()
			defer connectionPool.Close()

			// Set up a connection to the server.
			conn, err := connectionPool.GetConnection("localhost:8190")
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to connect")
			}
			defer conn.Close()
			c := proto.NewInternalAPIClient(conn)

			// Contact the server and print out its response.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			r, err := c.UpdateStatus(ctx, cs)

			if err != nil {
				log.Error().Err(err).Msg("Could not update the cluster status")
			} else {
				log.Info().
					Interface("res", r).
					Msg("Updated cluster status")
			}
		},
	}
)

// Execute executes the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().Uint32VarP(&shards, "shards", "s", 1, "Number of shards")
	rootCmd.Flags().StringArrayVarP(&staticNodes, "static-nodes", "n", nil, "Static list of nodes")
	rootCmd.Flags().Uint32VarP(&replicationFactor, "replication-factor", "r", 1, "The replication factor")

	//
	//rootCmd.PersistentFlags().StringVarP(&userLicense, "license", "l", "", "name of license for the project")
	rootCmd.PersistentFlags().BoolVarP(&logDebug, "log-debug", "d", false, "Enable debug logs")
	rootCmd.PersistentFlags().BoolVarP(&logJson, "log-json", "j", false, "Print logs in JSON format")
	//
	//rootCmd.AddCommand(addCmd)
	//rootCmd.AddCommand(initCmd)
}
