package crd

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	apiExtensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCrd(t *testing.T) {
	_fake := fake.NewSimpleClientset()
	client := &clientImpl{clientset: _fake}

	var out bytes.Buffer
	err := client.Install(&out)
	assert.NoError(t, err)
	assert.Equal(t, "CRD install succeeded\n", out.String())
	out.Reset()

	_crd, err := _fake.ApiextensionsV1().CustomResourceDefinitions().
		Get(context.Background(), name, v1.GetOptions{})

	assert.NoError(t, err)
	assert.Equal(t, "oxiaclusters.oxia.streamnative.io", _crd.Name)
	_spec := _crd.Spec
	assert.Equal(t, "oxia.streamnative.io", _spec.Group)
	names := _spec.Names
	assert.Equal(t, "OxiaCluster", names.Kind)
	assert.Equal(t, "oxiacluster", names.Singular)
	assert.Equal(t, "oxiaclusters", names.Plural)
	assert.Equal(t, []string{"oc"}, names.ShortNames)
	assert.Equal(t, apiExtensionsV1.NamespaceScoped, _spec.Scope)
	versions := _spec.Versions
	assert.Len(t, versions, 1)
	version := versions[0]
	assert.Equal(t, "v1alpha1", version.Name)
	assert.True(t, version.Served)
	assert.True(t, version.Storage)
	assert.NotNil(t, version.Subresources.Status)

	err = client.Uninstall(&out)
	assert.NoError(t, err)
	assert.Equal(t, "CRD uninstall succeeded\n", out.String())

	list, err := _fake.ApiextensionsV1().CustomResourceDefinitions().
		List(context.Background(), v1.ListOptions{})

	assert.NoError(t, err)
	assert.Len(t, list.Items, 0)
}
