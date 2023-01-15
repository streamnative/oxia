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

package kubernetes

import (
	"fmt"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"go.uber.org/multierr"
	"k8s.io/client-go/kubernetes"
	"oxia/pkg/apis/oxia/v1alpha1"
)

type ClusterClient interface {
	Apply(cluster v1alpha1.OxiaCluster) error
	Delete(namespace, name string, monitoringEnabled bool) error
}

type clusterClientImpl struct {
	kubernetes kubernetes.Interface
	monitoring monitoring.Interface
}

func NewClusterClient(kubernetes kubernetes.Interface, monitoring monitoring.Interface) ClusterClient {
	return &clusterClientImpl{
		kubernetes: kubernetes,
		monitoring: monitoring,
	}
}

func (c *clusterClientImpl) Apply(cluster v1alpha1.OxiaCluster) error {
	var errs error

	err := c.applyCoordinator(cluster)
	errs = multierr.Append(errs, err)

	err = c.applyServers(cluster)
	errs = multierr.Append(errs, err)

	return errs
}

func (c *clusterClientImpl) applyCoordinator(cluster v1alpha1.OxiaCluster) error {
	var errs error

	_, err := ServiceAccounts(c.kubernetes).Upsert(cluster.Namespace, serviceAccount(Coordinator, cluster))
	errs = multierr.Append(errs, err)

	_, err = Roles(c.kubernetes).Upsert(cluster.Namespace, role(cluster))
	errs = multierr.Append(errs, err)

	_, err = RoleBindings(c.kubernetes).Upsert(cluster.Namespace, roleBinding(cluster))
	errs = multierr.Append(errs, err)

	_, err = ConfigMaps(c.kubernetes).Upsert(cluster.Namespace, configMap(cluster))
	errs = multierr.Append(errs, err)

	_, err = Deployments(c.kubernetes).Upsert(cluster.Namespace, coordinatorDeployment(cluster))
	errs = multierr.Append(errs, err)

	_, err = Services(c.kubernetes).Upsert(cluster.Namespace, service(Coordinator, cluster, CoordinatorPorts))
	errs = multierr.Append(errs, err)

	if cluster.Spec.MonitoringEnabled {
		_, err = ServiceMonitors(c.monitoring).Upsert(cluster.Namespace, serviceMonitor(Coordinator, cluster))
		errs = multierr.Append(errs, err)
	}

	//TODO PodDisruptionBudget

	return errs
}

func (c *clusterClientImpl) applyServers(cluster v1alpha1.OxiaCluster) error {
	var errs error

	_, err := ServiceAccounts(c.kubernetes).Upsert(cluster.Namespace, serviceAccount(Server, cluster))
	errs = multierr.Append(errs, err)

	get, err := ServiceAccounts(c.kubernetes).Get(cluster.Namespace, cluster.Name)
	if get != nil {
		fmt.Printf("%+v\n", get.ObjectMeta.ResourceVersion)
	}
	if err != nil {
		return err
	}

	_, err = StatefulSets(c.kubernetes).Upsert(cluster.Namespace, serverStatefulSet(cluster))
	errs = multierr.Append(errs, err)

	_, err = Services(c.kubernetes).Upsert(cluster.Namespace, service(Server, cluster, ServerPorts))
	errs = multierr.Append(errs, err)

	if cluster.Spec.MonitoringEnabled {
		_, err = ServiceMonitors(c.monitoring).Upsert(cluster.Namespace, serviceMonitor(Server, cluster))
		errs = multierr.Append(errs, err)
	}

	//TODO PodDisruptionBudget

	return errs
}

func (c *clusterClientImpl) Delete(namespace, name string, monitoringEnabled bool) error {
	var errs error

	err := c.deleteServers(namespace, name, monitoringEnabled)
	errs = multierr.Append(errs, err)

	err = c.deleteCoordinator(namespace, name, monitoringEnabled)
	errs = multierr.Append(errs, err)

	return errs
}

func (c *clusterClientImpl) deleteCoordinator(namespace, name string, monitoringEnabled bool) error {
	var errs error

	name = name + "-coordinator"

	if monitoringEnabled {
		err := ServiceMonitors(c.monitoring).Delete(namespace, name)
		errs = multierr.Append(errs, err)
	}

	err := Services(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = Deployments(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = ConfigMaps(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = RoleBindings(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = Roles(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = ServiceAccounts(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	return errs
}

func (c *clusterClientImpl) deleteServers(namespace, name string, monitoringEnabled bool) error {
	var errs error

	if monitoringEnabled {
		err := ServiceMonitors(c.monitoring).Delete(namespace, name)
		errs = multierr.Append(errs, err)
	}

	err := Services(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = StatefulSets(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	err = ServiceAccounts(c.kubernetes).Delete(namespace, name)
	errs = multierr.Append(errs, err)

	return errs
}
