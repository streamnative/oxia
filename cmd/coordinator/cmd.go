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
	"log/slog"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/streamnative/oxia/common"

	"github.com/streamnative/oxia/cmd/flag"
	"github.com/streamnative/oxia/coordinator"
	"github.com/streamnative/oxia/coordinator/model"
)

var (
	conf       = coordinator.NewConfig()
	configFile string

	Cmd = &cobra.Command{
		Use:     "coordinator",
		Short:   "Start a coordinator",
		Long:    `Start a coordinator`,
		PreRunE: validate,
		RunE:    exec,
	}
)

func init() {
	flag.InternalAddr(Cmd, &conf.InternalServiceAddr)
	flag.MetricsAddr(Cmd, &conf.MetricsServiceAddr)
	Cmd.Flags().Var(&conf.MetadataProviderImpl, "metadata", "Metadata provider implementation: file, configmap or memory")
	Cmd.Flags().StringVar(&conf.K8SMetadataNamespace, "k8s-namespace", conf.K8SMetadataNamespace, "Kubernetes namespace for oxia config maps")
	Cmd.Flags().StringVar(&conf.K8SMetadataConfigMapName, "k8s-configmap-name", conf.K8SMetadataConfigMapName, "ConfigMap name for cluster status configmap")
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
	return nil
}

func configIsRemote() bool {
	return strings.HasPrefix(configFile, "configmap:")
}

func setConfigPath(v *viper.Viper) error {
	v.SetConfigType("yaml")

	if configIsRemote() {
		err := v.AddRemoteProvider("configmap", "endpoint", configFile)
		if err != nil {
			slog.Error("Failed to add remote provider", slog.Any("error", err))
			return err
		}

		return v.WatchRemoteConfigOnChannel()
	}

	if configFile == "" {
		v.AddConfigPath("/oxia/conf")
		v.AddConfigPath(".")
	}

	v.SetConfigFile(configFile)
	v.WatchConfig()
	return nil
}

func loadClusterConfig(v *viper.Viper) (model.ClusterConfig, error) {
	cc := model.ClusterConfig{}

	var err error

	if configIsRemote() {
		err = v.ReadRemoteConfig()
	} else {
		err = v.ReadInConfig()
	}

	if err != nil {
		return cc, err
	}

	if err := v.Unmarshal(&cc); err != nil {
		return cc, err
	}

	return cc, nil
}

func exec(*cobra.Command, []string) error {
	v := viper.New()

	conf.ClusterConfigChangeNotifications = make(chan any)
	conf.ClusterConfigProvider = func() (model.ClusterConfig, error) {
		return loadClusterConfig(v)
	}

	v.OnConfigChange(func(e fsnotify.Event) {
		conf.ClusterConfigChangeNotifications <- nil
	})

	if err := setConfigPath(v); err != nil {
		return err
	}

	if _, err := loadClusterConfig(v); err != nil {
		return err
	}

	common.RunProcess(func() (io.Closer, error) {
		return coordinator.New(conf)
	})
	return nil
}
