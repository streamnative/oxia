package cluster

import (
	"github.com/spf13/cobra"
	"oxia/operator/resource/cluster"
)

var (
	conf = cluster.NewConfig()

	Cmd = &cobra.Command{
		Use:   "cluster",
		Short: "Administer clusters",
		Long:  `Administer clusters`,
	}

	applyCmd = &cobra.Command{
		Use:     "apply",
		Short:   "Create/update a cluster",
		Long:    `Create/update a cluster`,
		PreRunE: validate,
		RunE:    apply,
	}

	deleteCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete a cluster",
		Long:  `Delete a cluster`,
		RunE:  _delete,
	}
)

func init() {
	Cmd.AddCommand(applyCmd)
	Cmd.AddCommand(deleteCmd)

	applyCmd.Flags().StringVar(&conf.Name, "name", conf.Name, "Cluster name")
	applyCmd.Flags().StringVar(&conf.Namespace, "namespace", conf.Namespace, "Cluster namespace")
	applyCmd.Flags().Uint32Var(&conf.ShardCount, "shards", conf.ShardCount, "The number of shards")
	applyCmd.Flags().Uint32Var(&conf.ReplicationFactor, "replication-factor", conf.ReplicationFactor, "The replication factor")
	applyCmd.Flags().Uint32Var(&conf.ServerReplicas, "servers", conf.ServerReplicas, "The number of servers")
	applyCmd.Flags().StringVar(&conf.ServerResources.Cpu, "server-cpu", conf.ServerResources.Cpu, "Server CPU")
	applyCmd.Flags().StringVar(&conf.ServerResources.Memory, "server-memory", conf.ServerResources.Memory, "Server memory")
	applyCmd.Flags().StringVar(&conf.ServerVolume, "server-volume", conf.ServerVolume, "Server volume")
	applyCmd.Flags().StringVar(&conf.CoordinatorResources.Cpu, "coordinator-cpu", conf.CoordinatorResources.Cpu, "Coordinator CPU")
	applyCmd.Flags().StringVar(&conf.CoordinatorResources.Memory, "coordinator-memory", conf.CoordinatorResources.Memory, "Coordinator memory")
	applyCmd.Flags().StringVar(&conf.Image, "image", conf.Image, "Oxia docker image")
	applyCmd.Flags().BoolVar(&conf.MonitoringEnabled, "monitoring-enabled", conf.MonitoringEnabled, "Prometheus ServiceMonitor")
}

func validate(*cobra.Command, []string) error {
	return conf.Validate()
}

func apply(cmd *cobra.Command, _ []string) error {
	return cluster.NewClient().Apply(cmd.OutOrStdout(), conf)
}

func _delete(cmd *cobra.Command, _ []string) error {
	return cluster.NewClient().Delete(cmd.OutOrStdout(), conf)
}
