package coordinator

import (
	"errors"
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
		PreRunE: validate,
		Run:     exec,
	}
)

func init() {
	flag.InternalPort(Cmd, &conf.InternalServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
	Cmd.Flags().Var(&conf.MetadataProviderImpl, "metadata", "Metadata provider implementation: memory or configmap")
	Cmd.Flags().StringVar(&conf.MetadataNamespace, "namespace", conf.MetadataNamespace, "Kubernetes namespace for metadata configmap")
	Cmd.Flags().StringVar(&conf.MetadataName, "name", conf.MetadataName, "ConfigMap name for metadata configmap")
	Cmd.Flags().StringVarP(&configFile, "conf", "f", "", "Cluster config file")
}

func validate(*cobra.Command, []string) error {
	if conf.MetadataProviderImpl == coordinator.Configmap {
		if conf.MetadataNamespace == "" {
			return errors.New("namespace must be set with metadata=configmap")
		}
		if conf.MetadataName == "" {
			return errors.New("name must be set with metadata=configmap")
		}
	}
	if err := loadClusterConfig(); err != nil {
		return err
	}
	return nil
}

func loadClusterConfig() error {
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
