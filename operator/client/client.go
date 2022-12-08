package client

import (
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"github.com/rs/zerolog/log"
	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
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
