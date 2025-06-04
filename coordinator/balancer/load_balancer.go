// Copyright 2025 StreamNative, Inc.
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

package balancer

import (
	"io"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"golang.org/x/net/context"

	"github.com/streamnative/oxia/coordinator/model"
)

type Options struct {
	context.Context

	CandidatesSupplier        func() *linkedhashset.Set
	CandidateMetadataSupplier func() map[string]model.ServerMetadata
	NamespaceConfigSupplier   func(namespace string) *model.NamespaceConfig
	StatusSupplier            func() *model.ClusterStatus
}

type LoadBalancer interface {
	io.Closer

	Action() <-chan Action
}
