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

package metrics

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"oxia/common/metrics"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"time"
)

type Metrics struct {
	timeFunc  func() time.Time
	sinceFunc func(time.Time) time.Duration

	opTime  Timer
	opValue instrument.Int64Histogram

	batchTotalTime Timer
	batchExecTime  Timer
	batchValue     instrument.Int64Histogram
	batchRequests  instrument.Int64Histogram
}

func NewMetrics(provider metric.MeterProvider) *Metrics {
	return newMetrics(provider, time.Now, time.Since)
}

func newMetrics(provider metric.MeterProvider, timeFunc func() time.Time, sinceFunc func(time.Time) time.Duration) *Metrics {
	meter := provider.Meter("oxia_client")
	return &Metrics{
		timeFunc:  timeFunc,
		sinceFunc: sinceFunc,

		opTime:  newTimer(meter, "oxia_client_op"),
		opValue: newHistogram(meter, "oxia_client_op_value", metrics.Bytes),

		batchTotalTime: newTimer(meter, "oxia_client_batch_total"),
		batchExecTime:  newTimer(meter, "oxia_client_batch_exec"),
		batchValue:     newHistogram(meter, "oxia_client_batch_value", metrics.Bytes),
		batchRequests:  newHistogram(meter, "oxia_client_batch_request", ""),
	}
}

func (m *Metrics) DecoratePut(put model.PutCall) model.PutCall {
	callback := put.Callback
	metricContext := m.metricContextFunc("put")
	put.Callback = func(response *proto.PutResponse, err error) {
		callback(response, err)
		ctx, start, _attrs := metricContext(err)
		m.opTime.Record(ctx, m.sinceFunc(start), _attrs...)
		m.opValue.Record(ctx, int64(len(put.Value)), _attrs...)
	}
	return put
}

func (m *Metrics) DecorateDelete(delete model.DeleteCall) model.DeleteCall {
	callback := delete.Callback
	metricContext := m.metricContextFunc("delete")
	delete.Callback = func(response *proto.DeleteResponse, err error) {
		callback(response, err)
		ctx, start, _attrs := metricContext(err)
		m.opTime.Record(ctx, m.sinceFunc(start), _attrs...)
	}
	return delete
}

func (m *Metrics) DecorateDeleteRange(deleteRange model.DeleteRangeCall) model.DeleteRangeCall {
	callback := deleteRange.Callback
	metricContext := m.metricContextFunc("delete_range")
	deleteRange.Callback = func(response *proto.DeleteRangeResponse, err error) {
		callback(response, err)
		ctx, start, _attrs := metricContext(err)
		m.opTime.Record(ctx, m.sinceFunc(start), _attrs...)
	}
	return deleteRange
}

func (m *Metrics) DecorateGet(get model.GetCall) model.GetCall {
	callback := get.Callback
	metricContext := m.metricContextFunc("get")
	get.Callback = func(response *proto.GetResponse, err error) {
		callback(response, err)
		ctx, start, _attrs := metricContext(err)
		m.opTime.Record(ctx, m.sinceFunc(start), _attrs...)
		var size int64 = 0
		if response != nil {
			size = int64(len(response.Value))
		}
		m.opValue.Record(ctx, size, _attrs...)
	}
	return get
}

func (m *Metrics) WriteCallback() func(time.Time, *proto.WriteRequest, *proto.WriteResponse, error) {
	metricContext := m.metricContextFunc("write")
	return func(executionStart time.Time, request *proto.WriteRequest, response *proto.WriteResponse, err error) {
		ctx, batchStart, _attrs := metricContext(err)
		m.batchTotalTime.Record(ctx, m.sinceFunc(batchStart), _attrs...)
		m.batchExecTime.Record(ctx, m.sinceFunc(executionStart), _attrs...)
		valueSize, requestCount := writeMetrics(request)
		m.batchValue.Record(ctx, valueSize, _attrs...)
		m.batchRequests.Record(ctx, requestCount, _attrs...)
	}
}

func (m *Metrics) ReadCallback() func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error) {
	metricContext := m.metricContextFunc("read")
	return func(executionStart time.Time, request *proto.ReadRequest, response *proto.ReadResponse, err error) {
		ctx, batchStart, attrs := metricContext(err)
		m.batchTotalTime.Record(ctx, m.sinceFunc(batchStart), attrs...)
		m.batchExecTime.Record(ctx, m.sinceFunc(executionStart), attrs...)
		valueSize, requestCount := readMetrics(response)
		m.batchValue.Record(ctx, valueSize, attrs...)
		m.batchRequests.Record(ctx, requestCount, attrs...)
	}
}

func (m *Metrics) metricContextFunc(requestType string) func(error) (context.Context, time.Time, []attribute.KeyValue) {
	start := m.timeFunc()
	return func(err error) (context.Context, time.Time, []attribute.KeyValue) {
		return context.TODO(), start, attrs(requestType, err)
	}
}
