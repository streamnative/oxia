// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	flag.InternalAddr(Cmd, &conf.InternalServiceAddr)
	flag.MetricsAddr(Cmd, &conf.MetricsServiceAddr)
	Cmd.Flags().Var(&conf.MetadataProviderImpl, "metadata", "Metadata provider implementation: file, configmap or memory")
	Cmd.Flags().StringVar(&conf.K8SMetadataNamespace, "k8s-namespace", conf.K8SMetadataNamespace, "Kubernetes namespace for metadata configmap")
	Cmd.Flags().StringVar(&conf.K8SMetadataConfigMapName, "k8s-configmap-name", conf.K8SMetadataConfigMapName, "ConfigMap name for metadata configmap")
	Cmd.Flags().StringVar(&conf.FileMetadataPath, "file-clusters-status-path", "data/cluster-status.json", "The path where the cluster status is stored when using 'file' provider")
	Cmd.Flags().StringVarP(&configFile, "conf", "f", "", "Cluster config file")
}

func validate(*cobra.Command, []string) error {
	if conf.MetadataProviderImpl == coordinator.Configmap {
		if conf.K8SMetadataNamespace == "" {
			return errors.New("k8s-namespace must be set with metadata=configmap")
		}
		if conf.K8SMetadataConfigMapName == "" {
			return errors.New("k8s-configmap-name must be set with metadata=configmap")
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
