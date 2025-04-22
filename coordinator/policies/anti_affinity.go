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

package policies

type AntiAffinityMode string

const (
	// Strict Enforce anti-affinity rules strictly. Ensemble choosing fails if rules can't be met.
	// This mode is for high-availability systems needing strict resource separation.
	Strict AntiAffinityMode = "Strict"

	// Relax Try to follow anti-affinity rules, but proceed with ensemble choosing if not feasible.
	// Ideal for environments where resource utilization is limited.
	Relax AntiAffinityMode = "Relax"
)

// AntiAffinity defines rules for keeping certain shards separated based on labels.
// It helps in achieving better fault tolerance by ensuring shards are distributed
// across different failure domains.
type AntiAffinity struct {

	// Labels specifies the set of labels to consider when making anti-affinity
	// decisions. Shards with matching labels will be scheduled according to
	// the anti-affinity rules.
	Labels []string `json:"labels,omitempty" yaml:"labels,omitempty"`

	// Mode specifies the execution mode of the anti-affinity, with the optional values of Strict or Relax.
	// This mode determines the behavior of the ensemble choosing when the anti-affinity rules cannot be satisfied.
	Mode AntiAffinityMode `json:"mode,omitempty" yaml:"mode,omitempty"`
}
