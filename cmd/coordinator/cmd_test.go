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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"os"
	"oxia/common"
	"oxia/coordinator"
	"oxia/coordinator/model"
	"strings"
	"testing"
)

func TestCmd(t *testing.T) {
	clusterConfig := model.ClusterConfig{
		Namespaces: map[string]model.NamespaceConfig{
			common.DefaultNamespace: {
				ReplicationFactor: 1,
				InitialShardCount: 2,
			},
		},
		Servers: []model.ServerAddress{{
			Public:   "public:1234",
			Internal: "internal:5678",
		}},
	}

	bytes, err := yaml.Marshal(&clusterConfig)
	assert.NoError(t, err)

	name := "config.yaml"

	err = os.WriteFile(name, bytes, os.ModePerm)
	assert.NoError(t, err)

	defer func() {
		err = os.Remove(name)
		assert.NoError(t, err)
	}()

	for _, test := range []struct {
		args         []string
		expectedConf coordinator.Config
		isErr        bool
	}{
		{[]string{}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:8080",
			MetadataProviderImpl: coordinator.File,
			ClusterConfig: model.ClusterConfig{
				Namespaces: map[string]model.NamespaceConfig{
					common.DefaultNamespace: {
						ReplicationFactor: 1,
						InitialShardCount: 2,
					},
				},
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-i=localhost:1234"}, coordinator.Config{
			InternalServiceAddr:  "localhost:1234",
			MetricsServiceAddr:   "localhost:8080",
			MetadataProviderImpl: coordinator.File,
			ClusterConfig: model.ClusterConfig{
				Namespaces: map[string]model.NamespaceConfig{
					common.DefaultNamespace: {
						ReplicationFactor: 1,
						InitialShardCount: 2,
					},
				},
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-i=0.0.0.0:1234"}, coordinator.Config{
			InternalServiceAddr:  "0.0.0.0:1234",
			MetricsServiceAddr:   "localhost:8080",
			MetadataProviderImpl: coordinator.File,
			ClusterConfig: model.ClusterConfig{
				Namespaces: map[string]model.NamespaceConfig{
					common.DefaultNamespace: {
						ReplicationFactor: 1,
						InitialShardCount: 2,
					},
				},
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-m=localhost:1234"}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:1234",
			MetadataProviderImpl: coordinator.File,
			ClusterConfig: model.ClusterConfig{
				Namespaces: map[string]model.NamespaceConfig{
					common.DefaultNamespace: {
						ReplicationFactor: 1,
						InitialShardCount: 2,
					},
				},
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-f=" + name}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:8080",
			MetadataProviderImpl: coordinator.File,
			ClusterConfig: model.ClusterConfig{
				Namespaces: map[string]model.NamespaceConfig{
					common.DefaultNamespace: {
						ReplicationFactor: 1,
						InitialShardCount: 2,
					},
				},
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-f=invalid.yaml"}, coordinator.Config{
			InternalServiceAddr: "localhost:6649",
			MetricsServiceAddr:  "localhost:8080",
			ClusterConfig:       model.ClusterConfig{}}, true},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			conf = coordinator.NewConfig()
			configFile = ""
			viper.Reset()
			Cmd.SetArgs(test.args)
			Cmd.Run = func(cmd *cobra.Command, args []string) {
				assert.Equal(t, test.expectedConf, conf)
			}
			err = Cmd.Execute()
			assert.Equal(t, test.isErr, err != nil)
		})
	}

	for _, test := range []struct {
		args  []string
		isErr bool
	}{
		{[]string{}, false},
		{[]string{"--metadata=memory"}, false},
		{[]string{"--metadata=configmap"}, true},
		{[]string{"--metadata=configmap", "--k8s-namespace=foo", "--k8s-configmap-name=bar"}, false},
		{[]string{"--metadata=configmap", "--k8s-namespace=foo}"}, true},
		{[]string{"--metadata=configmap", "--k8s-configmap-name=bar"}, true},
		{[]string{"--metadata=invalid"}, true},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			conf = coordinator.NewConfig()
			configFile = ""
			viper.Reset()
			Cmd.SetArgs(test.args)
			Cmd.Run = func(cmd *cobra.Command, args []string) {}
			err = Cmd.Execute()
			assert.Equal(t, test.isErr, err != nil)
		})
	}
}
