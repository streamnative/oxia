package coordinator

import (
	"github.com/spf13/cobra"
	"io"
	"oxia/cmd/flag"
	"oxia/common"
	"oxia/coordinator"
)

var (
	conf = coordinator.NewConfig()

	Cmd = &cobra.Command{
		Use:   "coordinator",
		Short: "Start a coordinator",
		Long:  `Start a coordinator`,
		Run:   exec,
	}
)

func init() {
	flag.InternalPort(Cmd, &conf.InternalServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
	Cmd.Flags().StringVar(&conf.Name, "name", conf.Name, "THe name of the Oxia cluster")
	Cmd.Flags().Uint32Var(&conf.ReplicationFactor, "replication-factor", conf.ReplicationFactor, "The replication factor")
	Cmd.Flags().Uint32Var(&conf.ShardCount, "shards", conf.ShardCount, "The number of shards")
	Cmd.Flags().Uint32Var(&conf.ServerReplicas, "servers", conf.ServerReplicas, "The number of servers")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return coordinator.New(conf)
	})
}
