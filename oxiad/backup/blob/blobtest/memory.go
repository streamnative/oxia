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

package blobtest

import (
	"context"
	"net/url"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// MemoryScheme is the URL scheme of the in-process stores, "mem://<name>",
// which this package registers for the tests that import it: a test opens
// the store of that name, shared by every opener in the process, with
// blob.Open. The name is the host and the path of the URL, so "mem://a/x"
// and "mem://a/y" are two stores. The scheme is not registered in the
// servers, so a policy cannot send the backups to the memory of the data
// servers.
const MemoryScheme = "mem"

func init() {
	blob.Register(MemoryScheme, func(_ context.Context, destination *url.URL) (blob.Store, error) {
		return blob.SharedMemory(destination.Host + destination.Path), nil
	})
}
