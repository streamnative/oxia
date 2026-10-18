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

package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestWithOutgoingMetadataMergesValues(t *testing.T) {
	ctx := metadata.AppendToOutgoingContext(context.Background(), "existing", "a", "replaced", "old")

	ctx = withOutgoingMetadata(ctx, func() map[string]string {
		return map[string]string{"replaced": "new", "added": "b"}
	})

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, []string{"a"}, md.Get("existing"))
	assert.Equal(t, []string{"new"}, md.Get("replaced"))
	assert.Equal(t, []string{"b"}, md.Get("added"))
}

func TestWithOutgoingMetadataKeepsContextWithoutValues(t *testing.T) {
	ctx := context.Background()

	result := withOutgoingMetadata(ctx, func() map[string]string { return nil })

	assert.Equal(t, ctx, result)
	_, ok := metadata.FromOutgoingContext(result)
	assert.False(t, ok)
}
