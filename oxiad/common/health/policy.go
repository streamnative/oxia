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

package health

import "time"

// CheckPolicy controls how the health checks distinguish a dead data server
// from one that is merely slow. A node that is saturated but healthy can miss
// individual probe deadlines; declaring it unavailable fences its shard
// leaders, moves the load elsewhere and makes the next node miss its probes in
// turn. Requiring several consecutive probe failures keeps that feedback loop
// from starting, while a deliberate NOT_SERVING report still fails the node
// immediately.
type CheckPolicy struct {
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
	// FailureThreshold is the number of consecutive failed probes after which
	// the data server is declared unavailable.
	FailureThreshold int
}

// DefaultCheckPolicy is the policy of the health checks of the coordinator.
var DefaultCheckPolicy = CheckPolicy{
	ProbeInterval:    2 * time.Second,
	ProbeTimeout:     5 * time.Second,
	FailureThreshold: 3,
}
