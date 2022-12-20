package coordinator

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"os"
	"oxia/coordinator"
	"oxia/coordinator/model"
	"strings"
	"testing"
)

func TestCmd(t *testing.T) {
	clusterConfig := model.ClusterConfig{
		ReplicationFactor: 1,
		ShardCount:        2,
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
			InternalServicePort:  6649,
			MetricsPort:          8080,
			MetadataProviderImpl: coordinator.Memory,
			ClusterConfig: model.ClusterConfig{
				ReplicationFactor: 1,
				ShardCount:        2,
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-i=1234"}, coordinator.Config{
			InternalServicePort:  1234,
			MetricsPort:          8080,
			MetadataProviderImpl: coordinator.Memory,
			ClusterConfig: model.ClusterConfig{
				ReplicationFactor: 1,
				ShardCount:        2,
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-m=1234"}, coordinator.Config{
			InternalServicePort:  6649,
			MetricsPort:          1234,
			MetadataProviderImpl: coordinator.Memory,
			ClusterConfig: model.ClusterConfig{
				ReplicationFactor: 1,
				ShardCount:        2,
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-f=" + name}, coordinator.Config{
			InternalServicePort:  6649,
			MetricsPort:          8080,
			MetadataProviderImpl: coordinator.Memory,
			ClusterConfig: model.ClusterConfig{
				ReplicationFactor: 1,
				ShardCount:        2,
				Servers: []model.ServerAddress{{
					Public:   "public:1234",
					Internal: "internal:5678",
				}}}}, false},
		{[]string{"-f=invalid.yaml"}, coordinator.Config{
			InternalServicePort: 6649,
			MetricsPort:         8080,
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
