package cluster

import (
	"errors"
	"fmt"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"go.uber.org/multierr"
	rbacV1 "k8s.io/api/rbac/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"oxia/operator/client"
	"oxia/operator/resource"
)

type Config struct {
	Name                 string
	Namespace            string
	ShardCount           uint32
	ReplicationFactor    uint32
	ServerReplicas       uint32
	ServerResources      resource.Resources
	ServerVolume         string
	CoordinatorResources resource.Resources
	Image                string
	MonitoringEnabled    bool
}

func NewConfig() Config {
	return Config{
		Name:              "",
		Namespace:         "",
		ShardCount:        3,
		ReplicationFactor: 3,
		ServerReplicas:    3,
		ServerResources: resource.Resources{
			Cpu:    "100m",
			Memory: "128Mi",
		},
		ServerVolume: "1Gi",
		CoordinatorResources: resource.Resources{
			Cpu:    "100m",
			Memory: "128Mi",
		},
		//TODO fully qualified and versioned image:tag
		Image:             "oxia:latest",
		MonitoringEnabled: true,
	}
}

func (c *Config) Validate() error {
	var errs error

	if c.Name == "" {
		errs = multierr.Append(errs, errors.New("name must be set"))
	}

	if c.Namespace == "" {
		errs = multierr.Append(errs, errors.New("namespace must be set"))
	}

	_, err := k8sResource.ParseQuantity(c.ServerResources.Cpu)
	errs = multierr.Append(errs, fmt.Errorf("ServerResources.Cpu: %s", err.Error()))

	_, err = k8sResource.ParseQuantity(c.ServerResources.Memory)
	errs = multierr.Append(errs, fmt.Errorf("ServerResources.Memory: %s", err.Error()))

	_, err = k8sResource.ParseQuantity(c.ServerVolume)
	errs = multierr.Append(errs, fmt.Errorf("ServerVolume: %s", err.Error()))

	_, err = k8sResource.ParseQuantity(c.CoordinatorResources.Cpu)
	errs = multierr.Append(errs, fmt.Errorf("ServerResources.Cpu: %s", err.Error()))

	_, err = k8sResource.ParseQuantity(c.CoordinatorResources.Memory)
	errs = multierr.Append(errs, fmt.Errorf("ServerResources.Memory: %s", err.Error()))

	return errs
}

type Client interface {
	Apply(config Config) error
	Delete(config Config) error
}

type clientImpl struct {
	kubernetes kubernetes.Interface
	monitoring monitoring.Interface
}

func NewClient() Client {
	config := client.NewConfig()
	return &clientImpl{
		kubernetes: client.NewKubernetesClientset(config),
		monitoring: client.NewMonitoringClientset(config),
	}
}

func (c *clientImpl) Apply(config Config) error {
	var errs error

	err := c.applyCoordinator(config)
	errs = multierr.Append(errs, err)

	err = c.applyServers(config)
	errs = multierr.Append(errs, err)

	return errs
}

func (c *clientImpl) applyCoordinator(config Config) error {
	var errs error

	name := config.Name + "-coordinator"
	ports := []resource.NamedPort{resource.MetricsPort}

	err := client.ServiceAccounts(c.kubernetes).Upsert(config.Namespace, resource.ServiceAccount(name))
	errs = multierr.Append(errs, err)

	err = client.Roles(c.kubernetes).Upsert(config.Namespace, role(name))
	errs = multierr.Append(errs, err)

	err = client.RoleBindings(c.kubernetes).Upsert(config.Namespace, roleBinding(name, config.Namespace))
	errs = multierr.Append(errs, err)

	deploymentConfig := resource.DeploymentConfig{
		PodConfig: resource.PodConfig{
			Name:      name,
			Image:     config.Image,
			Command:   "coordinator",
			Args:      []string{}, //TODO configure Args - ShardCount, ReplicationFactor
			Ports:     ports,
			Resources: config.CoordinatorResources,
		},
		Replicas: 1,
	}
	err = client.Deployments(c.kubernetes).Upsert(config.Namespace, resource.Deployment(deploymentConfig))
	errs = multierr.Append(errs, err)

	serviceConfig := resource.ServiceConfig{
		Name:     name,
		Headless: false,
		Ports:    ports,
	}
	err = client.Services(c.kubernetes).Upsert(config.Namespace, resource.Service(serviceConfig))
	errs = multierr.Append(errs, err)

	if config.MonitoringEnabled {
		err = client.ServiceMonitors(c.monitoring).Upsert(config.Namespace, resource.ServiceMonitor(name))
		errs = multierr.Append(errs, err)
	}

	//TODO PodDisruptionBudget

	return errs
}

func (c *clientImpl) applyServers(config Config) error {
	var errs error

	ports := resource.AllPorts

	err := client.ServiceAccounts(c.kubernetes).Upsert(config.Namespace, resource.ServiceAccount(config.Name))
	errs = multierr.Append(errs, err)

	statefulSetConfig := resource.StatefulSetConfig{
		PodConfig: resource.PodConfig{
			Name:      config.Name,
			Image:     config.Image,
			Command:   "server",
			Args:      []string{}, //TODO configure Args - ShardCount, ReplicationFactor
			Ports:     ports,
			Resources: config.ServerResources,
			VolumeConfig: &resource.VolumeConfig{
				Name:   "data",
				Path:   "/data",
				Volume: config.ServerVolume,
			},
		},
		Replicas: config.ServerReplicas,
		Volume:   config.ServerVolume,
	}
	err = client.StatefulSets(c.kubernetes).Upsert(config.Namespace, resource.StatefulSet(statefulSetConfig))
	errs = multierr.Append(errs, err)

	serviceConfig := resource.ServiceConfig{
		Name:     config.Name,
		Headless: true,
		Ports:    ports,
	}
	err = client.Services(c.kubernetes).Upsert(config.Namespace, resource.Service(serviceConfig))
	errs = multierr.Append(errs, err)

	if config.MonitoringEnabled {
		err = client.ServiceMonitors(c.monitoring).Upsert(config.Namespace, resource.ServiceMonitor(config.Name))
		errs = multierr.Append(errs, err)
	}

	//TODO PodDisruptionBudget

	return errs
}

func (c *clientImpl) Delete(config Config) error {
	var errs error

	err := c.deleteServers(config)
	errs = multierr.Append(errs, err)

	err = c.deleteCoordinator(config)
	errs = multierr.Append(errs, err)

	return errs
}

func (c *clientImpl) deleteCoordinator(config Config) error {
	var errs error

	name := config.Name + "-coordinator"

	if config.MonitoringEnabled {
		err := client.ServiceMonitors(c.monitoring).Delete(config.Namespace, name)
		errs = multierr.Append(errs, err)
	}

	err := client.Services(c.kubernetes).Delete(config.Namespace, name)
	errs = multierr.Append(errs, err)

	err = client.Deployments(c.kubernetes).Delete(config.Namespace, name)
	errs = multierr.Append(errs, err)

	err = client.RoleBindings(c.kubernetes).Delete(config.Namespace, name)
	errs = multierr.Append(errs, err)

	err = client.Roles(c.kubernetes).Delete(config.Namespace, name)
	errs = multierr.Append(errs, err)

	err = client.ServiceAccounts(c.kubernetes).Delete(config.Namespace, name)
	errs = multierr.Append(errs, err)

	return errs
}

func (c *clientImpl) deleteServers(config Config) error {
	var errs error

	if config.MonitoringEnabled {
		err := client.ServiceMonitors(c.monitoring).Delete(config.Namespace, config.Name)
		errs = multierr.Append(errs, err)
	}

	err := client.Services(c.kubernetes).Delete(config.Namespace, config.Name)
	errs = multierr.Append(errs, err)

	err = client.StatefulSets(c.kubernetes).Delete(config.Namespace, config.Name)
	errs = multierr.Append(errs, err)

	err = client.ServiceAccounts(c.kubernetes).Delete(config.Namespace, config.Name)
	errs = multierr.Append(errs, err)

	return errs
}

func role(name string) *rbacV1.Role {
	return &rbacV1.Role{
		ObjectMeta: resource.Meta(name),
		Rules:      policyRules(),
	}
}

func policyRules() []rbacV1.PolicyRule {
	return []rbacV1.PolicyRule{
		//If storing shard state on the OxiaCluster status
		resource.PolicyRule("oxia.streamnative.io", []string{"oxiaclusters"}, []string{"get", "update"}),
		//If storing shard state on a configmap data
		resource.PolicyRule("", []string{"configmaps"}, []string{"*"}),
	}
}

func roleBinding(name, namespace string) *rbacV1.RoleBinding {
	return &rbacV1.RoleBinding{
		ObjectMeta: resource.Meta(name),
		Subjects: []rbacV1.Subject{{
			Kind:      "ServiceAccount",
			Name:      name,
			Namespace: namespace,
		}},
		RoleRef: rbacV1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     name,
		},
	}
}
