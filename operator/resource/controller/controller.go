package controller

import (
	"context"
	"fmt"
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"go.uber.org/multierr"
	"io"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
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

	err := c.upsertServiceAccount(config.Namespace, serviceAccount())
	errs = printAndAppend(out, errs, err, "install", "ServiceAccount")

	err = c.upsertClusterRole(clusterRole())
	errs = printAndAppend(out, errs, err, "install", "ClusterRole")

	err = c.upsertClusterRoleBinding(clusterRoleBinding(config.Namespace))
	errs = printAndAppend(out, errs, err, "install", "ClusterRoleBinding")

	err = c.upsertDeployment(config.Namespace, deployment(config))
	errs = printAndAppend(out, errs, err, "install", "Deployment")

	if config.MonitoringEnabled {
		err = c.upsertService(config.Namespace, service())
		errs = printAndAppend(out, errs, err, "install", "Service")

		err = c.upsertServiceMonitor(config.Namespace, serviceMonitor())
		errs = printAndAppend(out, errs, err, "install", "ServiceMonitor")
	}

	//TODO PodDisruptionBudget

	return errs
}

func (c *clientImpl) Uninstall(out io.Writer, config Config) error {
	var errs error

	//TODO PodDisruptionBudget

	if config.MonitoringEnabled {
		err := c.monitoring.MonitoringV1().ServiceMonitors(config.Namespace).
			Delete(context.Background(), name, metaV1.DeleteOptions{})
		errs = printAndAppend(out, errs, err, "uninstall", "ServiceMonitor")

		err = c.kubernetes.CoreV1().Services(config.Namespace).
			Delete(context.Background(), name, metaV1.DeleteOptions{})
		errs = printAndAppend(out, errs, err, "uninstall", "Service")
	}

	err := c.kubernetes.AppsV1().Deployments(config.Namespace).
		Delete(context.Background(), name, metaV1.DeleteOptions{})
	errs = printAndAppend(out, errs, err, "uninstall", "Deployment")

	err = c.kubernetes.RbacV1().ClusterRoleBindings().
		Delete(context.Background(), name, metaV1.DeleteOptions{})
	errs = printAndAppend(out, errs, err, "uninstall", "ClusterRoleBinding")

	err = c.kubernetes.RbacV1().ClusterRoles().
		Delete(context.Background(), name, metaV1.DeleteOptions{})
	errs = printAndAppend(out, errs, err, "uninstall", "ClusterRole")

	err = c.kubernetes.CoreV1().ServiceAccounts(config.Namespace).
		Delete(context.Background(), name, metaV1.DeleteOptions{})
	errs = printAndAppend(out, errs, err, "uninstall", "ServiceAccount")

	return errs
}

func (c *clientImpl) upsertServiceAccount(namespace string, serviceAccount *coreV1.ServiceAccount) (err error) {
	_client := c.kubernetes.CoreV1().ServiceAccounts(namespace)
	_, err = _client.Update(context.Background(), serviceAccount, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = _client.Create(context.Background(), serviceAccount, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl) upsertClusterRole(clusterRole *rbacV1.ClusterRole) (err error) {
	_client := c.kubernetes.RbacV1().ClusterRoles()
	_, err = _client.Update(context.Background(), clusterRole, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = _client.Create(context.Background(), clusterRole, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl) upsertClusterRoleBinding(clusterRoleBinding *rbacV1.ClusterRoleBinding) (err error) {
	_client := c.kubernetes.RbacV1().ClusterRoleBindings()
	_, err = _client.Update(context.Background(), clusterRoleBinding, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = _client.Create(context.Background(), clusterRoleBinding, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl) upsertDeployment(namespace string, deployment *appsV1.Deployment) (err error) {
	_client := c.kubernetes.AppsV1().Deployments(namespace)
	_, err = _client.Update(context.Background(), deployment, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = _client.Create(context.Background(), deployment, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl) upsertService(namespace string, service *coreV1.Service) (err error) {
	_client := c.kubernetes.CoreV1().Services(namespace)
	_, err = _client.Update(context.Background(), service, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = _client.Create(context.Background(), service, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl) upsertServiceMonitor(namespace string, serviceMonitor *monitoringV1.ServiceMonitor) (err error) {
	_client := c.monitoring.MonitoringV1().ServiceMonitors(namespace)
	_, err = _client.Update(context.Background(), serviceMonitor, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = _client.Create(context.Background(), serviceMonitor, metaV1.CreateOptions{})
	}
	return
}

func printAndAppend(out io.Writer, errs error, err error, operation string, resource string) error {
	if err == nil {
		_, _ = fmt.Fprintf(out, "%s %s succeeded\n", resource, operation)
		return nil
	} else {
		_, _ = fmt.Fprintf(out, "%s %s failed\n", resource, operation)
		return multierr.Append(errs, err)
	}
}

func serviceAccount() *coreV1.ServiceAccount {
	return &coreV1.ServiceAccount{
		ObjectMeta: resource.Meta(name),
	}
}

func clusterRole() *rbacV1.ClusterRole {
	return &rbacV1.ClusterRole{
		ObjectMeta: resource.Meta(name),
		Rules: []rbacV1.PolicyRule{
			policyRule(crd.Group, []string{crd.Resource}, "*"),
			policyRule("apps", []string{"deployments", "statefulsets"}, "*"),
			policyRule("", []string{"services"}, "*"),
			policyRule("monitoring.coreos.com", []string{"servicemonitors"}, "*"),
		},
	}
}

func policyRule(apiGroup string, resources []string, verbs ...string) rbacV1.PolicyRule {
	return rbacV1.PolicyRule{
		APIGroups: []string{apiGroup},
		Resources: resources,
		Verbs:     verbs,
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

func deployment(config Config) *appsV1.Deployment {
	resourceList := resource.List(config.Resources)
	return &appsV1.Deployment{
		ObjectMeta: resource.Meta(name),
		Spec: appsV1.DeploymentSpec{
			Replicas: pointer.Int32(1),
			Selector: &metaV1.LabelSelector{MatchLabels: resource.Labels(name)},
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: resource.Meta(name),
				Spec: coreV1.PodSpec{
					ServiceAccountName: name,
					Containers: []coreV1.Container{{
						Name:            name,
						Command:         []string{"oxia", "controller"},
						Image:           config.Image,
						ImagePullPolicy: coreV1.PullIfNotPresent,
						Ports: []coreV1.ContainerPort{
							resource.ContainerPort(resource.MetricsPortName, resource.MetricsPort()),
						},
						Resources: coreV1.ResourceRequirements{
							Limits:   resourceList,
							Requests: resourceList,
						},
						LivenessProbe:  probe(),
						ReadinessProbe: probe(),
					}},
				},
			},
		},
	}
}

func probe() *coreV1.Probe {
	return &coreV1.Probe{
		ProbeHandler: coreV1.ProbeHandler{
			GRPC: &coreV1.GRPCAction{
				Port: int32(resource.InternalPort()),
			},
		},
	}
}

func service() *coreV1.Service {
	return &coreV1.Service{
		ObjectMeta: resource.Meta(name),
		Spec: coreV1.ServiceSpec{
			Selector: resource.Labels(name),
			Ports:    resource.Transform(resource.Ports, resource.ServicePort),
		},
	}
}

func serviceMonitor() *monitoringV1.ServiceMonitor {
	return &monitoringV1.ServiceMonitor{
		ObjectMeta: resource.Meta(name),
		Spec: monitoringV1.ServiceMonitorSpec{
			Selector:  metaV1.LabelSelector{MatchLabels: resource.Labels(name)},
			Endpoints: []monitoringV1.Endpoint{{Port: resource.MetricsPortName}},
		},
	}
}
