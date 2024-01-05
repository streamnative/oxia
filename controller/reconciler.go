// Copyright 2024 StreamNative, Inc.
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

package controller

import (
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	k "oxia/kubernetes"
	"oxia/pkg/apis/oxia/v1alpha1"
)

type Reconciler interface {
	Reconcile(watch.Event)
}

type reconciler struct {
	client k.ClusterClient
	log    zerolog.Logger
}

func newReconciler(kubernetes kubernetes.Interface, monitoring monitoring.Interface) Reconciler {
	return &reconciler{
		client: k.NewClusterClient(kubernetes, monitoring),
		log: log.With().
			Str("component", "reconciler").
			Logger(),
	}
}

func (r *reconciler) Reconcile(event watch.Event) {
	switch event.Type {
	case watch.Added:
		cluster := event.Object.(*v1alpha1.OxiaCluster)
		err := r.client.Apply(*cluster)
		if err != nil {
			r.log.Error().Err(err).
				Str("namespace", cluster.Namespace).
				Str("name", cluster.Name).
				Msg("failed to create cluster")
		} else {
			r.log.Info().
				Str("namespace", cluster.Namespace).
				Str("name", cluster.Name).
				Msg("created cluster")
		}
	case watch.Deleted:
		cluster := event.Object.(*v1alpha1.OxiaCluster)
		err := r.client.Delete(cluster.Namespace, cluster.Name, cluster.Spec.MonitoringEnabled)
		if err != nil {
			r.log.Error().Err(err).
				Str("namespace", cluster.Namespace).
				Str("name", cluster.Name).
				Msg("failed to delete cluster")
		} else {
			r.log.Info().
				Str("namespace", cluster.Namespace).
				Str("name", cluster.Name).
				Msg("deleted cluster")
		}
	default:
		r.log.Warn().
			Str("eventType", string(event.Type)).
			Interface("eventObject", event.Object).
			Msg("unexpected event")
	}
}
