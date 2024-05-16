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
	"io"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/streamnative/oxia/cmd/flag"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator"
	"github.com/streamnative/oxia/coordinator/model"
)

var (
	conf           = coordinator.NewConfig()
	configFile     string
	configChangeCh chan struct{}

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
	Cmd.Flags().DurationVar(&conf.ClusterConfigRefreshTime, "conf-file-refresh-time", 1*time.Minute, "How frequently to check for updates for cluster configuration file")

	setConfigPath()
	viper.OnConfigChange(func(_ fsnotify.Event) {
		configChangeCh <- struct{}{}
	})
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
	if _, _, err := loadClusterConfig(); err != nil {
		return err
	}
	return nil
}

func setConfigPath() {
	if configFile == "" {
		viper.AddConfigPath("/oxia/conf")
		viper.AddConfigPath(".")
	} else {
		viper.SetConfigFile(configFile)
	}
}

func loadClusterConfig() (model.ClusterConfig, chan struct{}, error) {
	setConfigPath()
	cc := model.ClusterConfig{}

	if err := viper.ReadInConfig(); err != nil {
		return cc, configChangeCh, err
	}

	if err := viper.Unmarshal(&cc); err != nil {
		return cc, configChangeCh, err
	}

	return cc, configChangeCh, nil
}

func exec(*cobra.Command, []string) {
	viper.WatchConfig()
	conf.ClusterConfigProvider = loadClusterConfig

	common.RunProcess(func() (io.Closer, error) {
		return coordinator.New(conf)
	})
}
