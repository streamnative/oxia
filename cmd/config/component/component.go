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

package component

import (
	"fmt"

	"github.com/oxia-db/oxia/common/codec"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

// NewOptions returns the default options of the named component, before WithDefault.
func NewOptions(name string) (codec.ConfigurableOptions, error) {
	switch name {
	case "dataserver":
		return dataserveroption.NewDefaultOptions(), nil
	case "coordinator":
		return coordinatoroption.NewDefaultOptions(), nil
	default:
		return nil, fmt.Errorf("unknown component %q: use dataserver or coordinator", name)
	}
}
