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

package impl

import (
	"context"
	"log/slog"
	"os"

	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func NewK8SClientConfig() *rest.Config {
	kubeconfigGetter := clientcmd.NewDefaultClientConfigLoadingRules().Load
	config, err := clientcmd.BuildConfigFromKubeconfigGetter("", kubeconfigGetter)
	if err != nil {
		slog.Error(
			"failed to load kubeconfig",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	return config
}

//
// func NewOxiaClientset(config *rest.Config) oxia.Interface {
//	clientset, err := oxia.NewForConfig(config)
//	if err != nil {
//		log.Fatal().Err(err).Msg("failed to create client")
//	}
//	return clientset
//}

func NewK8SClientset(config *rest.Config) kubernetes.Interface {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Error(
			"failed to create client",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	return clientset
}

func K8SConfigMaps(kubernetes kubernetes.Interface) Client[coreV1.ConfigMap] {
	return newNamespaceClient[coreV1.ConfigMap](func(namespace string) ResourceInterface[coreV1.ConfigMap] {
		return kubernetes.CoreV1().ConfigMaps(namespace)
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
	Get(ctx context.Context, name string, opts metaV1.GetOptions) (*Resource, error)
}

type resource interface {
	coreV1.ConfigMap
}

type Client[Resource resource] interface {
	Upsert(namespace string, resource *Resource) (*Resource, error)
	Delete(namespace, name string) error
	Get(namespace, name string) (*Resource, error)
}

type clientImpl[Resource resource] struct {
	clientFunc func(string) ResourceInterface[Resource]
}

func (c *clientImpl[Resource]) Upsert(namespace string, resource *Resource) (result *Resource, err error) {
	client := c.clientFunc(namespace)
	result, err = client.Update(context.Background(), resource, metaV1.UpdateOptions{})
	if errors.IsConflict(err) {
		return
	}
	if errors.IsNotFound(err) {
		result, err = client.Create(context.Background(), resource, metaV1.CreateOptions{})
	}
	return
}

func (c *clientImpl[Resource]) Delete(namespace, name string) error {
	client := c.clientFunc(namespace)
	return client.Delete(context.Background(), name, metaV1.DeleteOptions{})
}

func (c *clientImpl[Resource]) Get(namespace, name string) (*Resource, error) {
	client := c.clientFunc(namespace)
	return client.Get(context.Background(), name, metaV1.GetOptions{})
}
