package controller

import (
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"io"
	rbacV1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/kubernetes"
	"oxia/operator/client"
	"oxia/operator/resource"
	"oxia/operator/resource/crd"
)

var name = "oxia-controller"

type Config struct {
	Namespace         string
	MonitoringEnabled bool
	Image             string
	Resources         resource.Resources
}

func NewConfig() Config {
	return Config{
		Namespace:         "",
		MonitoringEnabled: false,
		//TODO fully qualified and versioned image:tag
		Image: "oxia:latest",
		Resources: resource.Resources{
			Cpu:    "100m",
			Memory: "128Mi",
		},
	}
}

type Client interface {
	Install(out io.Writer, config Config) error
	Uninstall(out io.Writer, config Config) error
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

func (c *clientImpl) Install(out io.Writer, config Config) error {
	var errs error

	err := client.ServiceAccounts(c.kubernetes).Upsert(config.Namespace, resource.ServiceAccount(name))
	errs = resource.PrintAndAppend(out, errs, err, "install", "ServiceAccount")

	err = client.ClusterRoles(c.kubernetes).Upsert(clusterRole(name))
	errs = resource.PrintAndAppend(out, errs, err, "install", "ClusterRole")

	err = client.ClusterRoleBindings(c.kubernetes).Upsert(clusterRoleBinding(config.Namespace))
	errs = resource.PrintAndAppend(out, errs, err, "install", "ClusterRoleBinding")

	deploymentConfig := resource.DeploymentConfig{
		Name:      name,
		Image:     config.Image,
		Command:   "controller",
		Replicas:  1,
		Ports:     []resource.NamedPort{resource.MetricsPort},
		Resources: config.Resources,
	}
	err = client.Deployments(c.kubernetes).Upsert(config.Namespace, resource.Deployment(deploymentConfig))
	errs = resource.PrintAndAppend(out, errs, err, "install", "Deployment")

	if config.MonitoringEnabled {
		serviceConfig := resource.ServiceConfig{
			Name:     name,
			Headless: false,
			Ports:    []resource.NamedPort{resource.MetricsPort},
		}
		err = client.Services(c.kubernetes).Upsert(config.Namespace, resource.Service(serviceConfig))
		errs = resource.PrintAndAppend(out, errs, err, "install", "Service")

		err = client.ServiceMonitors(c.monitoring).Upsert(config.Namespace, resource.ServiceMonitor(name))
		errs = resource.PrintAndAppend(out, errs, err, "install", "ServiceMonitor")
	}

	//TODO PodDisruptionBudget

	return errs
}

func (c *clientImpl) Uninstall(out io.Writer, config Config) error {
	var errs error

	//TODO PodDisruptionBudget

	if config.MonitoringEnabled {
		err := client.ServiceMonitors(c.monitoring).Delete(config.Namespace, name)
		errs = resource.PrintAndAppend(out, errs, err, "uninstall", "ServiceMonitor")

		err = client.Services(c.kubernetes).Delete(config.Namespace, name)
		errs = resource.PrintAndAppend(out, errs, err, "uninstall", "Service")
	}

	err := client.Deployments(c.kubernetes).Delete(config.Namespace, name)
	errs = resource.PrintAndAppend(out, errs, err, "uninstall", "Deployment")

	err = client.ClusterRoleBindings(c.kubernetes).Delete(name)
	errs = resource.PrintAndAppend(out, errs, err, "uninstall", "ClusterRoleBinding")

	err = client.ClusterRoles(c.kubernetes).Delete(name)
	errs = resource.PrintAndAppend(out, errs, err, "uninstall", "ClusterRole")

	err = client.ServiceAccounts(c.kubernetes).Delete(config.Namespace, name)
	errs = resource.PrintAndAppend(out, errs, err, "uninstall", "ServiceAccount")

	return errs
}

func clusterRole(name string) *rbacV1.ClusterRole {
	return &rbacV1.ClusterRole{
		ObjectMeta: resource.Meta(name),
		Rules:      policyRules(),
	}
}

func policyRules() []rbacV1.PolicyRule {
	return []rbacV1.PolicyRule{
		resource.PolicyRule(crd.Group, []string{crd.Resource}, []string{"*"}),
		resource.PolicyRule("apps", []string{"deployments", "statefulsets"}, []string{"*"}),
		resource.PolicyRule("", []string{"services"}, []string{"*"}),
		resource.PolicyRule("monitoring.coreos.com", []string{"servicemonitors"}, []string{"*"}),
	}
}

func clusterRoleBinding(namespace string) *rbacV1.ClusterRoleBinding {
	return &rbacV1.ClusterRoleBinding{
		ObjectMeta: resource.Meta(name),
		Subjects: []rbacV1.Subject{{
			APIGroup:  "rbac.authorization.k8s.io",
			Kind:      "ServiceAccount",
			Name:      name,
			Namespace: namespace,
		}},
		RoleRef: rbacV1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     crd.Resource,
		},
	}
}
