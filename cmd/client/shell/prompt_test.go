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

package shell

import (
	"testing"

	prompt "github.com/c-bata/go-prompt"
	"github.com/stretchr/testify/assert"
)

func TestCompleteFiltersPartialWord(t *testing.T) {
	assertSuggestionTexts(t, complete(documentFor("ge")), "get")
	assertSuggestionTexts(t, complete(documentFor("admin da")), "dataserver")
	assertSuggestionTexts(t, complete(documentFor("admin namespace c")), "create")
}

func TestCompleteSuggestsNextWordAfterSpace(t *testing.T) {
	assertSuggestionTexts(t, complete(documentFor("admin ")), "namespace", "dataserver", "shard")
	assertSuggestionTexts(t, complete(documentFor("delete ")), "--expected-version", "--partition-key")
}

func TestSuggestionsForReturnsNothingForUnknownPositions(t *testing.T) {
	assert.Empty(t, suggestionsFor([]string{"admin", "cluster"}))
	assert.Empty(t, suggestionsFor([]string{"get", "key"}))
	assert.Empty(t, suggestionsFor([]string{"get", "key", "extra"}))
	assert.Empty(t, suggestionsFor([]string{"admin", "namespace", "create", "ns"}))
}

func TestAdminFlagSuggestionsCoversResources(t *testing.T) {
	assertSuggestionTexts(t, suggestionsFor([]string{"admin", "namespaces", "patch"}),
		"--output", "--replication-factor", "--notifications")
	assertSuggestionTexts(t, suggestionsFor([]string{"admin", "data-servers", "create"}),
		"--output", "--public", "--internal", "--label")
	assertSuggestionTexts(t, suggestionsFor([]string{"admin", "shards", "split"}),
		"--namespace", "--shard", "--split-point")
	assert.Empty(t, suggestionsFor([]string{"admin", "shard", "merge"}))
	assert.Empty(t, suggestionsFor([]string{"admin", "cluster", "list"}))
}

func documentFor(text string) prompt.Document {
	buffer := prompt.NewBuffer()
	buffer.InsertText(text, false, true)
	return *buffer.Document()
}
