package kubernetes

import (
	"context"
	monitoringV1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"github.com/rs/zerolog/log"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"oxia/pkg/apis/oxia/v1alpha1"
	oxia "oxia/pkg/generated/clientset/versioned"
)

func NewClientConfig() *rest.Config {
	kubeconfigGetter := clientcmd.NewDefaultClientConfigLoadingRules().Load
	config, err := clientcmd.BuildConfigFromKubeconfigGetter("", kubeconfigGetter)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load kubeconfig")
	}
	return config
}

func NewOxiaClientset(config *rest.Config) oxia.Interface {
	clientset, err := oxia.NewForConfig(config)
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

func Roles(kubernetes kubernetes.Interface) Client[rbacV1.Role] {
	return newNamespaceClient[rbacV1.Role](func(namespace string) ResourceInterface[rbacV1.Role] {
		return kubernetes.RbacV1().Roles(namespace)
	})
}

func RoleBindings(kubernetes kubernetes.Interface) Client[rbacV1.RoleBinding] {
	return newNamespaceClient[rbacV1.RoleBinding](func(namespace string) ResourceInterface[rbacV1.RoleBinding] {
		return kubernetes.RbacV1().RoleBindings(namespace)
	})
}

func OxiaClusters(oxia oxia.Interface) Client[v1alpha1.OxiaCluster] {
	return newNamespaceClient[v1alpha1.OxiaCluster](func(namespace string) ResourceInterface[v1alpha1.OxiaCluster] {
		return oxia.OxiaV1alpha1().OxiaClusters(namespace)
	})
}

func ServiceAccounts(kubernetes kubernetes.Interface) Client[coreV1.ServiceAccount] {
	return newNamespaceClient[coreV1.ServiceAccount](func(namespace string) ResourceInterface[coreV1.ServiceAccount] {
		return kubernetes.CoreV1().ServiceAccounts(namespace)
	})
}

func Services(kubernetes kubernetes.Interface) Client[coreV1.Service] {
	return newNamespaceClient[coreV1.Service](func(namespace string) ResourceInterface[coreV1.Service] {
		return kubernetes.CoreV1().Services(namespace)
	})
}

func ConfigMaps(kubernetes kubernetes.Interface) Client[coreV1.ConfigMap] {
	return newNamespaceClient[coreV1.ConfigMap](func(namespace string) ResourceInterface[coreV1.ConfigMap] {
		return kubernetes.CoreV1().ConfigMaps(namespace)
	})
}

func Deployments(kubernetes kubernetes.Interface) Client[appsV1.Deployment] {
	return newNamespaceClient[appsV1.Deployment](func(namespace string) ResourceInterface[appsV1.Deployment] {
		return kubernetes.AppsV1().Deployments(namespace)
	})
}

func StatefulSets(kubernetes kubernetes.Interface) Client[appsV1.StatefulSet] {
	return newNamespaceClient[appsV1.StatefulSet](func(namespace string) ResourceInterface[appsV1.StatefulSet] {
		return kubernetes.AppsV1().StatefulSets(namespace)
	})
}

func ServiceMonitors(monitoring monitoring.Interface) Client[monitoringV1.ServiceMonitor] {
	return newNamespaceClient[monitoringV1.ServiceMonitor](func(namespace string) ResourceInterface[monitoringV1.ServiceMonitor] {
		return monitoring.MonitoringV1().ServiceMonitors(namespace)
	})
}

func newNamespaceClient[Resource resource](clientFunc func(string) ResourceInterface[Resource]) Client[Resource] {
	return &clientImpl[Resource]{
		clientFunc: clientFunc,
	}
}

type ResourceInterface[Resource resource] interface {
	Create(ctx context.Context, Resource *Resource, opts metaV1.CreateOptions) (*Resource, error)
	Update(ctx context.Context, Resource *Resource, opts metaV1.UpdateOptions) (*Resource, error)
	Delete(ctx context.Context, name string, opts metaV1.DeleteOptions) error
}

type resource interface {
	v1alpha1.OxiaCluster |
		rbacV1.Role |
		rbacV1.RoleBinding |
		coreV1.ServiceAccount |
		coreV1.Service |
		coreV1.ConfigMap |
		appsV1.Deployment |
		appsV1.StatefulSet |
		monitoringV1.ServiceMonitor
}

type Client[Resource resource] interface {
	Upsert(namespace string, resource *Resource) error
	Delete(namespace, name string) error
}

type clientImpl[Resource resource] struct {
	clientFunc func(string) ResourceInterface[Resource]
}

func (c *clientImpl[Resource]) Upsert(namespace string, resource *Resource) (err error) {
	client := c.clientFunc(namespace)
	_, err = client.Update(context.Background(), resource, metaV1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = client.Create(context.Background(), resource, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl[Resource]) Delete(namespace, name string) error {
	client := c.clientFunc(namespace)
	return client.Delete(context.Background(), name, metaV1.DeleteOptions{})
}
