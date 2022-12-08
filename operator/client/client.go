package client

import (
	"github.com/rs/zerolog/log"
	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
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
