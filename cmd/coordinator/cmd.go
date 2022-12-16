package coordinator

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io"
	"oxia/cmd/flag"
	"oxia/common"
	"oxia/coordinator"
)

var (
	conf       = coordinator.NewConfig()
	configFile string

	Cmd = &cobra.Command{
		Use:     "coordinator",
		Short:   "Start a coordinator",
		Long:    `Start a coordinator`,
		PreRunE: loadClusterConfig,
		Run:     exec,
	}
)

func init() {
	flag.InternalPort(Cmd, &conf.InternalServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
	Cmd.Flags().StringVarP(&configFile, "conf", "f", "", "Cluster config file")
}

func loadClusterConfig(*cobra.Command, []string) error {
	if configFile == "" {
		viper.AddConfigPath("/oxia/conf")
		viper.AddConfigPath(".")
	} else {
		viper.SetConfigFile(configFile)
	}

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	return viper.Unmarshal(&conf.ClusterConfig)
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return coordinator.New(conf)
	})
}
