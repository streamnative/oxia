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

package backup

import (
	"slices"
	"time"
)

// Expired returns, among the backups of a namespace, the ones to expire at
// now under the retention: a backup expires once it is older than the
// retention, and a newer backup, or a set of newer backups, covering its
// whole hash range is older than the retention too. Until then it is the
// newest backup of some part of its range as of the start of the retention
// window, which a restore to that time needs.
//
// The newer backups of a range include the backups of the same shard, and
// those of the children of a split, which cover the range of their parent
// between them.
func Expired(backups []Summary, retention time.Duration, now time.Time) []Summary {
	cutoff := now.Add(-retention)
	// The backups older than the retention, newest first: only they can
	// expire, and only they can expire another one.
	old := make([]Summary, 0, len(backups))
	for _, b := range backups {
		if b.ID.Time.Before(cutoff) {
			old = append(old, b)
		}
	}
	slices.SortFunc(old, func(a, b Summary) int { return b.ID.Compare(a.ID) })

	var expired []Summary
	for i, b := range old {
		// The newer ones precede b in the order
		if coversRange(old[:i], b.Range) {
			expired = append(expired, b)
		}
	}
	return expired
}

// coversRange reports whether the union of the ranges of the backups covers
// the whole range.
func coversRange(backups []Summary, r Range) bool {
	ranges := make([]Range, 0, len(backups))
	for _, b := range backups {
		if b.Range.Overlaps(r) {
			ranges = append(ranges, b.Range)
		}
	}
	slices.SortFunc(ranges, func(a, b Range) int {
		if a.Min != b.Min {
			if a.Min < b.Min {
				return -1
			}
			return 1
		}
		return 0
	})
	// next is the first point of r not covered yet
	next := uint64(r.Min)
	for _, cover := range ranges {
		if uint64(cover.Min) > next {
			return false
		}
		if end := uint64(cover.Max) + 1; end > next {
			next = end
		}
		if next > uint64(r.Max) {
			return true
		}
	}
	return next > uint64(r.Max)
}
