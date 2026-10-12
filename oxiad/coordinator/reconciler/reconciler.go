// Copyright 2023-2026 The Oxia Authors
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

package reconciler

import (
	"context"
	"io"

	"github.com/oxia-db/oxia/common/proto"
)

// Reconciler brings the coordinator runtime in line with the cluster
// configuration: the data servers and namespaces of the configuration are
// created in the runtime, and the ones that are no longer in it are deleted.
//
// The data server and namespace reconcilers each handle their part, and the
// cluster reconciler, which New returns, runs them all on the configuration,
// first when it starts, then on each of its changes.
type Reconciler interface {
	io.Closer

	// Reconcile makes one pass over the given configuration, and returns its
	// errors without retrying. A pass that fails on one data server or
	// namespace still handles the others, and a pass can be repeated: it only
	// changes what doesn't match the configuration yet. The caller retries a
	// failed pass, e.g. with a backoff.
	Reconcile(context.Context, *proto.ClusterConfiguration) error
}
