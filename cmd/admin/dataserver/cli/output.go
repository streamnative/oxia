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

package cli

import (
	"io"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

type dataServerViewOutput struct {
	DataServer       *proto.DataServer       `json:"data_server,omitempty" yaml:"dataserver,omitempty"`
	DataServerStatus *dataServerStatusOutput `json:"data_server_status,omitempty" yaml:"dataserverstatus,omitempty"`
}

type dataServerStatusOutput struct {
	State             string   `json:"state,omitempty" yaml:"state,omitempty"`
	SupportedFeatures []string `json:"supported_features,omitempty" yaml:"supportedfeatures,omitempty"`
}

func WriteDataServers(out io.Writer, format string, dataServers []*proto.DataServerView) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	if commons.NormalizeOutputFormat(format) == commons.OutputTable {
		return writeDataServerStatusTable(out, dataServers)
	}

	values := make([]dataServerViewOutput, 0, len(dataServers))
	for _, view := range dataServers {
		if view == nil {
			continue
		}
		values = append(values, dataServerViewOutput{
			DataServer:       view.GetDataServer(),
			DataServerStatus: dataServerStatusOutputFor(view.GetDataServerStatus()),
		})
	}
	return commons.WriteStructuredOutput(out, format, values)
}

func WriteDataServer(out io.Writer, format string, dataServer *proto.DataServer) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	if commons.NormalizeOutputFormat(format) == commons.OutputTable {
		return writeDataServerConfigTable(out, []*proto.DataServer{dataServer})
	}
	return commons.WriteStructuredOutput(out, format, dataServer)
}

func WriteDataServerView(out io.Writer, format string, dataServer *proto.DataServerView) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	if commons.NormalizeOutputFormat(format) == commons.OutputTable {
		return writeDataServerStatusTable(out, []*proto.DataServerView{dataServer})
	}

	value := dataServerViewOutput{}
	if dataServer != nil {
		value.DataServer = dataServer.GetDataServer()
		value.DataServerStatus = dataServerStatusOutputFor(dataServer.GetDataServerStatus())
	}
	return commons.WriteStructuredOutput(out, format, value)
}

func writeDataServerConfigTable(out io.Writer, dataServers []*proto.DataServer) error {
	rows := make([][]string, 0, len(dataServers))
	for _, dataServer := range dataServers {
		if dataServer == nil {
			continue
		}
		identity := dataServer.GetIdentity()
		rows = append(rows, []string{
			dataServer.GetNameOrDefault(),
			identity.GetPublic(),
			identity.GetInternal(),
			commons.FormatLabels(dataServer.GetMetadata().GetLabels()),
		})
	}
	return commons.WriteTable(out, []string{"NAME", "PUBLIC", "INTERNAL", "LABELS"}, rows)
}

func dataServerStatusOutputFor(status *proto.DataServerStatus) *dataServerStatusOutput {
	if status == nil {
		return nil
	}
	features := status.GetSupportedFeatures()
	featureNames := make([]string, 0, len(features))
	for _, feature := range features {
		featureNames = append(featureNames, feature.String())
	}
	return &dataServerStatusOutput{
		State:             status.GetState().String(),
		SupportedFeatures: featureNames,
	}
}

func writeDataServerStatusTable(out io.Writer, dataServers []*proto.DataServerView) error {
	rows := make([][]string, 0, len(dataServers))
	for _, view := range dataServers {
		if view == nil || view.GetDataServer() == nil {
			continue
		}
		dataServer := view.GetDataServer()
		identity := dataServer.GetIdentity()
		rows = append(rows, []string{
			dataServer.GetNameOrDefault(),
			identity.GetPublic(),
			identity.GetInternal(),
			view.GetDataServerStatus().GetState().String(),
		})
	}
	return commons.WriteTable(out, []string{"NAME", "PUBLIC", "INTERNAL", "STATE"}, rows)
}
