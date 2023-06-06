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

package common

import (
	"context"
	"github.com/go-logr/logr"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NamedPort struct {
	Name string
	Port int
}

var (
	PublicPort   = NamedPort{"public", 6648}
	InternalPort = NamedPort{"internal", 6649}
	MetricsPort  = NamedPort{"metrics", 8080}
)

type Component string

var (
	Coordinator Component = "coordinator"
	Server      Component = "server"
)

var FieldOwner = "oxia-controller"

type SubReconcilerContext struct {
	client.Client
	Log      logr.Logger
	InnerCtx context.Context
}

type Reconcile func(ctx *SubReconcilerContext, oxia *oxiav1alpha1.OxiaCluster) error
