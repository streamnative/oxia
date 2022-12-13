package cluster

import (
	"io"
	"oxia/operator/client"
	"oxia/operator/resource"
	oxiav1alpha1 "oxia/pkg/apis/oxia/v1alpha1"
	oxia "oxia/pkg/generated/clientset/versioned"
)

type ManagedClient interface {
	Apply(out io.Writer, config Config) error
	Delete(out io.Writer, config Config) error
}

type managedClientImpl struct {
	oxia oxia.Interface
}

func NewManagedClient() ManagedClient {
	config := client.NewConfig()
	return &managedClientImpl{
		oxia: client.NewOxiaClientset(config),
	}
}

func (c *managedClientImpl) Apply(out io.Writer, config Config) error {
	var errs error
	err := client.OxiaClusters(c.oxia).Upsert(config.Namespace, oxiaCluster(config))
	errs = resource.PrintAndAppend(out, errs, err, "apply", "OxiaCluster")
	return errs
}

func (c *managedClientImpl) Delete(out io.Writer, config Config) error {
	var errs error
	err := client.OxiaClusters(c.oxia).Delete(config.Namespace, config.Name)
	errs = resource.PrintAndAppend(out, errs, err, "delete", "OxiaCluster")
	return errs
}

func oxiaCluster(config Config) *oxiav1alpha1.OxiaCluster {
	return &oxiav1alpha1.OxiaCluster{
		ObjectMeta: resource.Meta(config.Name),
		Spec: oxiav1alpha1.OxiaClusterSpec{
			ShardCount:        &config.ShardCount,
			ReplicationFactor: &config.ReplicationFactor,
			ServerReplicas:    &config.ServerReplicas,
			ServerResources: oxiav1alpha1.Resources{
				Cpu:    config.ServerResources.Cpu,
				Memory: config.ServerResources.Memory,
			},
			ServerVolume: config.ServerVolume,
			CoordinatorResources: oxiav1alpha1.Resources{
				Cpu:    config.CoordinatorResources.Cpu,
				Memory: config.CoordinatorResources.Memory,
			},
			Image:             config.Image,
			MonitoringEnabled: config.MonitoringEnabled,
		},
	}
}
