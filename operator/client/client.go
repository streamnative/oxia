package client

import (
	"context"
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"github.com/rs/zerolog/log"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func NewConfig() *rest.Config {
	kubeconfigGetter := clientcmd.NewDefaultClientConfigLoadingRules().Load
	config, err := clientcmd.BuildConfigFromKubeconfigGetter("", kubeconfigGetter)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load kubeconfig")
	}
	return config
}

func NewApiExtensionsClientset(config *rest.Config) apiExtensions.Interface {
	clientset, err := apiExtensions.NewForConfig(config)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create client")
	}
	return clientset
}

func NewKubernetesClientset(config *rest.Config) kubernetes.Interface {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create client")
	}
	return clientset
}

func NewMonitoringClientset(config *rest.Config) monitoring.Interface {
	clientset, err := monitoring.NewForConfig(config)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create client")
	}
	return clientset
}

func ClusterRoles(kubernetes kubernetes.Interface) ClusterClient[rbacV1.ClusterRole] {
	return newClusterClient[rbacV1.ClusterRole](kubernetes.RbacV1().ClusterRoles())
}

func ClusterRoleBindings(kubernetes kubernetes.Interface) ClusterClient[rbacV1.ClusterRoleBinding] {
	return newClusterClient[rbacV1.ClusterRoleBinding](kubernetes.RbacV1().ClusterRoleBindings())
}

func ServiceAccounts(kubernetes kubernetes.Interface) NamespaceClient[coreV1.ServiceAccount] {
	return newNamespaceClient[coreV1.ServiceAccount](func(namespace string) ResourceInterface[coreV1.ServiceAccount] {
		return kubernetes.CoreV1().ServiceAccounts(namespace)
	})
}

func Services(kubernetes kubernetes.Interface) NamespaceClient[coreV1.Service] {
	return newNamespaceClient[coreV1.Service](func(namespace string) ResourceInterface[coreV1.Service] {
		return kubernetes.CoreV1().Services(namespace)
	})
}

func Deployments(kubernetes kubernetes.Interface) NamespaceClient[appsV1.Deployment] {
	return newNamespaceClient[appsV1.Deployment](func(namespace string) ResourceInterface[appsV1.Deployment] {
		return kubernetes.AppsV1().Deployments(namespace)
	})
}

func ServiceMonitors(monitoring monitoring.Interface) NamespaceClient[monitoringV1.ServiceMonitor] {
	return newNamespaceClient[monitoringV1.ServiceMonitor](func(namespace string) ResourceInterface[monitoringV1.ServiceMonitor] {
		return monitoring.MonitoringV1().ServiceMonitors(namespace)
	})
}

func newClusterClient[Resource resource](client ResourceInterface[Resource]) ClusterClient[Resource] {
	return &clusterClientImpl[Resource]{
		client: client,
	}
}

func newNamespaceClient[Resource resource](clientFunc func(string) ResourceInterface[Resource]) NamespaceClient[Resource] {
	return &namespaceClientImpl[Resource]{
		clientFunc: clientFunc,
	}
}

type ResourceInterface[Resource resource] interface {
	Create(ctx context.Context, Resource *Resource, opts metaV1.CreateOptions) (*Resource, error)
	Update(ctx context.Context, Resource *Resource, opts metaV1.UpdateOptions) (*Resource, error)
	Delete(ctx context.Context, name string, opts metaV1.DeleteOptions) error
}

type resource interface {
	//cluster scoped
	rbacV1.ClusterRole |
		rbacV1.ClusterRoleBinding |
		//namespace scoped
		coreV1.ServiceAccount |
		coreV1.Service |
		appsV1.Deployment |
		monitoringV1.ServiceMonitor
}

type ClusterClient[Resource resource] interface {
	Upsert(resource *Resource) error
	Delete(name string) error
}

type clusterClientImpl[Resource resource] struct {
	client ResourceInterface[Resource]
}

func (c *clusterClientImpl[Resource]) Upsert(resource *Resource) error {
	return _upsert(c.client, resource)
}

func (c *clusterClientImpl[Resource]) Delete(name string) error {
	return _delete(c.client, name)
}

type NamespaceClient[Resource resource] interface {
	Upsert(namespace string, resource *Resource) error
	Delete(namespace, name string) error
}

type namespaceClientImpl[Resource resource] struct {
	clientFunc func(string) ResourceInterface[Resource]
}

func (c *namespaceClientImpl[Resource]) Upsert(namespace string, resource *Resource) error {
	return _upsert(c.clientFunc(namespace), resource)
}

func (c *namespaceClientImpl[Resource]) Delete(namespace, name string) error {
	return _delete(c.clientFunc(namespace), name)
}

func _upsert[Resource resource](client ResourceInterface[Resource], resource *Resource) (err error) {
	_, err = client.Update(context.Background(), resource, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = client.Create(context.Background(), resource, metaV1.CreateOptions{})
	}
	return
}

func _delete[Resource resource](client ResourceInterface[Resource], name string) error {
	return client.Delete(context.Background(), name, metaV1.DeleteOptions{})
}
