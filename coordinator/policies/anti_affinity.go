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

// UnsatisfiableAction defines the behavior when policy constraints cannot be satisfied.
type UnsatisfiableAction string

const (
	// DoNotSchedule instructs the coordinator to not schedule the shard when constraints
	// cannot be satisfied. This is the more conservative approach that ensures policy
	// compliance at the cost of potentially leaving shards unscheduled.
	DoNotSchedule UnsatisfiableAction = "DoNotSchedule"

	// ScheduleAnyway instructs the coordinator to proceed with scheduling the shard even
	// when constraints cannot be satisfied. This is a more permissive approach that
	// ensures service availability at the cost of potentially violating policy constraints.
	ScheduleAnyway UnsatisfiableAction = "ScheduleAnyway"
)

// AntiAffinity defines rules for keeping certain shards separated based on labels.
// It helps in achieving better fault tolerance by ensuring shards are distributed
// across different failure domains.
type AntiAffinity struct {

	// Labels specifies the set of labels to consider when making anti-affinity
	// decisions. Shards with matching labels will be scheduled according to
	// the anti-affinity rules.
	Labels []string `json:"labels,omitempty" yaml:"labels,omitempty"`

	// UnsatisfiableAction determines what action to take when anti-affinity
	// constraints cannot be satisfied. It can be either DoNotSchedule or
	// ScheduleAnyway.
	UnsatisfiableAction UnsatisfiableAction `json:"UnsatisfiableAction,omitempty" yaml:"unsatisfiableAction,omitempty"`
}
