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
	"github.com/streamnative/oxia/oxia"
	"time"
)

var (
	Config = ClientConfig{}
)

type ClientConfig struct {
	ServiceAddr         string
	Namespace           string
	BatchLinger         time.Duration
	MaxRequestsPerBatch int
	RequestTimeout      time.Duration
}

func (config *ClientConfig) NewClient() (oxia.AsyncClient, error) {
	return oxia.NewAsyncClient(Config.ServiceAddr,
		oxia.WithBatchLinger(Config.BatchLinger),
		oxia.WithRequestTimeout(Config.RequestTimeout),
		oxia.WithMaxRequestsPerBatch(Config.MaxRequestsPerBatch),
		oxia.WithNamespace(Config.Namespace),
	)
}
