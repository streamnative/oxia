// Copyright 2023 StreamNative, Inc.
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

package common

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type OptBooleanDefaultTrue struct {
	val *bool
}

func Bool(val bool) OptBooleanDefaultTrue {
	return OptBooleanDefaultTrue{&val}
}

func (o *OptBooleanDefaultTrue) Get() bool {
	if o.val != nil {
		return *o.val
	}

	return true
}

func (o *OptBooleanDefaultTrue) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.val)
}

var trueVal = true
var falseVal = false

func (o *OptBooleanDefaultTrue) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == "null" || s == "" || s == `""` {
		o.val = &trueVal
		return nil
	}

	if s == "true" {
		o.val = &trueVal
		return nil
	}

	if s == "false" {
		o.val = &falseVal
		return nil
	}

	return errors.New("invalid boolean value: " + s)
}
