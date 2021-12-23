package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"oxia/common"
	"oxia/proto"
	"time"
)

func main() {
	common.ConfigureLogger(false, false)

	connectionPool := common.NewConnectionPool()
	defer connectionPool.Close()

	// Set up a connection to the server.
	conn, err := connectionPool.GetConnection("localhost:8190")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect")
	}
	c := proto.NewInternalAPIClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.UpdateStatus(ctx, &proto.ClusterStatus{
		ReplicationFactor: 1,
		ShardsStatus: []*proto.ShardStatus{
			{
				Shard: 0,
				Leader: &proto.ServerAddress{
					InternalUrl: "matteo-laptop:8190",
					PublicUrl:   "matteo-laptop:9190",
				},
				Followers: []*proto.ServerAddress{
					{
						InternalUrl: "localhost:8192",
						PublicUrl:   "localhost:9192",
					},
					{
						InternalUrl: "localhost:8193",
						PublicUrl:   "localhost:9193",
					},
				},
				Epochs: []*proto.EpochStatus{
					{
						Epoch:      0,
						FirstEntry: 0,
					},
					{
						Epoch:      1,
						FirstEntry: 12,
					},
				},
			},
			{
				Shard: 1,
				Leader: &proto.ServerAddress{
					InternalUrl: "other:8190",
					PublicUrl:   "other:9190",
				},
				Followers: []*proto.ServerAddress{
					{
						InternalUrl: "matteo-laptop:8190",
						PublicUrl:   "matteo-laptop:9190",
					},
				},
				Epochs: []*proto.EpochStatus{
					{
						Epoch:      0,
						FirstEntry: 0,
					},
					{
						Epoch:      1,
						FirstEntry: 12,
					},
				},
			},
		},
	})

	if err != nil {
		log.Error().Err(err).Msg("Could not update the cluster status")
	} else {
		log.Printf("Updated cluster status: %s", r.String())
	}
}
