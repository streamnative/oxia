package crd

import (
	"context"
	"fmt"
	"io"
	apiExtensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oxia/operator/client"
	"oxia/operator/resource"
)

var (
	Kind     = "OxiaCluster"
	Resource = "oxiaclusters"
	Group    = "oxia.streamnative.io"
	name     = Resource + "." + Group
	spec     = apiExtensionsV1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiExtensionsV1.JSONSchemaProps{
			"serverReplicas":    {Type: "integer"},
			"shardCount":        {Type: "integer"},
			"replicationFactor": {Type: "integer"},
		},
	}
	status = apiExtensionsV1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiExtensionsV1.JSONSchemaProps{
			"shards": {
				Type: "array",
				Items: &apiExtensionsV1.JSONSchemaPropsOrArray{
					Schema: &shardMetadata,
				},
			},
		}}
	shardMetadata = apiExtensionsV1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiExtensionsV1.JSONSchemaProps{
			"id":     {Type: "integer"},
			"status": {Type: "string"},
			"epoch":  {Type: "integer"},
			"leader": serverAddress,
			"ensemble": {
				Type: "array",
				Items: &apiExtensionsV1.JSONSchemaPropsOrArray{
					Schema: &serverAddress,
				},
			},
		},
	}
	serverAddress = apiExtensionsV1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiExtensionsV1.JSONSchemaProps{
			"public":   {Type: "string"},
			"internal": {Type: "string"},
		}}
)

type Client interface {
	Install(out io.Writer) error
	Uninstall(out io.Writer) error
}

type clientImpl struct {
	clientset apiExtensions.Interface
}

func NewClient() Client {
	config := client.NewConfig()
	return &clientImpl{
		clientset: client.NewApiExtensionsClientset(config),
	}
}

func (c *clientImpl) Install(out io.Writer) error {
	_, err := c.clientset.ApiextensionsV1().CustomResourceDefinitions().
		Create(context.Background(), crd(), metaV1.CreateOptions{})
	if err == nil {
		_, _ = fmt.Fprintln(out, "CRD install succeeded")
	} else {
		_, _ = fmt.Fprintln(out, "CRD install failed")
	}
	return err
}

func (c *clientImpl) Uninstall(out io.Writer) error {
	err := c.clientset.ApiextensionsV1().CustomResourceDefinitions().
		Delete(context.Background(), name, metaV1.DeleteOptions{})
	if err == nil {
		_, _ = fmt.Fprintln(out, "CRD uninstall succeeded")
	} else {
		_, _ = fmt.Fprintln(out, "CRD uninstall failed")
	}
	return err
}

func crd() *apiExtensionsV1.CustomResourceDefinition {
	return &apiExtensionsV1.CustomResourceDefinition{
		ObjectMeta: resource.Meta(name),
		Spec: apiExtensionsV1.CustomResourceDefinitionSpec{
			Group: Group,
			Names: apiExtensionsV1.CustomResourceDefinitionNames{
				Kind:       Kind,
				Singular:   "oxiacluster",
				Plural:     "oxiaclusters",
				ShortNames: []string{"oc"},
			},
			Scope: apiExtensionsV1.NamespaceScoped,
			Versions: []apiExtensionsV1.CustomResourceDefinitionVersion{
				{
					Name: "v1alpha1",
					Schema: &apiExtensionsV1.CustomResourceValidation{
						OpenAPIV3Schema: &apiExtensionsV1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiExtensionsV1.JSONSchemaProps{
								"spec":   spec,
								"status": status,
							},
						},
					},
					Served:  true,
					Storage: true,
					Subresources: &apiExtensionsV1.CustomResourceSubresources{
						Status: &apiExtensionsV1.CustomResourceSubresourceStatus{},
					},
				},
			},
		},
	}
}
