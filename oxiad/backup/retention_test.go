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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	retentionBase = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	leftHalf      = Range{Min: 0, Max: 1<<31 - 1}
	rightHalf     = Range{Min: 1 << 31, Max: FullRange.Max}
)

// summary makes the summary of a backup of a shard, taken hoursAgo hours
// before the test's now (retentionBase + 7 days).
func summary(shard int64, r Range, hoursAgo int) Summary {
	return Summary{
		Namespace: "ns",
		Shard:     shard,
		ID:        NewID(retentionNow().Add(-time.Duration(hoursAgo)*time.Hour), "lineage"),
		Range:     r,
	}
}

func retentionNow() time.Time {
	return retentionBase.Add(7 * 24 * time.Hour)
}

func ids(summaries []Summary) []string {
	out := make([]string, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, s.ID.String())
	}
	return out
}

func TestExpiredKeepsEveryBackupInsideTheWindow(t *testing.T) {
	backups := []Summary{summary(1, FullRange, 1), summary(1, FullRange, 10), summary(1, FullRange, 47)}
	assert.Empty(t, Expired(backups, 48*time.Hour, retentionNow()))
}

func TestExpiredKeepsTheNewestBackupBeforeTheWindow(t *testing.T) {
	backups := []Summary{summary(1, FullRange, 1), summary(1, FullRange, 50), summary(1, FullRange, 60)}
	expired := Expired(backups, 48*time.Hour, retentionNow())
	assert.Equal(t, ids([]Summary{summary(1, FullRange, 60)}), ids(expired))
}

func TestExpiredKeepsTheOnlyBackupOfAnIdleShard(t *testing.T) {
	backups := []Summary{summary(1, FullRange, 500)}
	assert.Empty(t, Expired(backups, 48*time.Hour, retentionNow()))
}

func TestExpiredExpiresParentOnceChildrenCoverItBeforeTheWindow(t *testing.T) {
	parent := summary(1, FullRange, 60)
	left := summary(2, leftHalf, 55)
	right := summary(3, rightHalf, 52)
	assert.Empty(t, Expired([]Summary{parent, left}, 48*time.Hour, retentionNow()),
		"the right half is only covered by the parent")
	expired := Expired([]Summary{parent, left, right}, 48*time.Hour, retentionNow())
	assert.Equal(t, ids([]Summary{parent}), ids(expired))
}

func TestExpiredKeepsParentWhileAChildBackupIsInsideTheWindow(t *testing.T) {
	parent := summary(1, FullRange, 60)
	left := summary(2, leftHalf, 55)
	right := summary(3, rightHalf, 10)
	assert.Empty(t, Expired([]Summary{parent, left, right}, 48*time.Hour, retentionNow()))
}

func TestExpiredExpiresChildCoveredByNewerParentBackup(t *testing.T) {
	// A restore of an older namespace state that was then split again
	// doesn't happen, but a range covered by any newer backups expires
	child := summary(2, leftHalf, 70)
	parent := summary(1, FullRange, 60)
	newer := summary(1, FullRange, 50)
	expired := Expired([]Summary{child, parent, newer}, 48*time.Hour, retentionNow())
	assert.Equal(t, ids([]Summary{parent, child}), ids(expired))
}

func TestExpiredBreaksTiesByLineageSoOneSurvives(t *testing.T) {
	at := retentionNow().Add(-60 * time.Hour)
	a := Summary{Namespace: "ns", Shard: 1, ID: NewID(at, "aaaa"), Range: FullRange}
	b := Summary{Namespace: "ns", Shard: 1, ID: NewID(at, "bbbb"), Range: FullRange}
	expired := Expired([]Summary{a, b}, 48*time.Hour, retentionNow())
	assert.Equal(t, ids([]Summary{a}), ids(expired))
}

func TestExpiredIgnoresBackupsOfOtherRanges(t *testing.T) {
	old := summary(2, leftHalf, 60)
	newer := summary(3, rightHalf, 50)
	assert.Empty(t, Expired([]Summary{old, newer}, 48*time.Hour, retentionNow()))
}

func TestCoversRange(t *testing.T) {
	quarter := uint32(1 << 30)
	q := func(i uint32) Range { return Range{Min: i * quarter, Max: (i+1)*quarter - 1} }
	ranges := func(rs ...Range) []Summary {
		out := make([]Summary, 0, len(rs))
		for _, r := range rs {
			out = append(out, Summary{Range: r})
		}
		return out
	}
	assert.True(t, coversRange(ranges(q(0), q(1), q(2), q(3)), FullRange))
	assert.True(t, coversRange(ranges(q(3), q(1), q(2), q(0)), FullRange), "any order")
	assert.True(t, coversRange(ranges(leftHalf, q(2), q(3)), FullRange))
	assert.True(t, coversRange(ranges(FullRange), q(2)))
	assert.False(t, coversRange(ranges(q(0), q(1), q(3)), FullRange), "gap")
	assert.False(t, coversRange(ranges(q(0), q(1)), FullRange))
	assert.False(t, coversRange(nil, q(0)))
	assert.True(t, coversRange(ranges(q(1), leftHalf), q(1)), "overlapping covers")
}
