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

//go:build linux || (darwin && !ios)

package metric

// The ARMv8 system counter ticks at a fixed rate, and Linux and macOS let user
// space read it: their clock_gettime does. iOS doesn't.
const counterOK = true

// readCounter reads CNTVCT_EL0.
func readCounter() int64
