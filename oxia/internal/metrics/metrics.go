package metrics

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"time"
)

type Metrics struct {
	timeFunc  func() time.Time
	sinceFunc func(time.Time) time.Duration

	opTime    Timer
	opPayload syncint64.Histogram

	batchTotalTime Timer
	batchExecTime  Timer
	batchPayload   syncint64.Histogram
	batchRequests  syncint64.Histogram
}

func NewMetrics(provider metric.MeterProvider) *Metrics {
	return newMetrics(provider, time.Now, time.Since)
}

func newMetrics(provider metric.MeterProvider, timeFunc func() time.Time, sinceFunc func(time.Time) time.Duration) *Metrics {
	meter := provider.Meter("oxia_client")
	return &Metrics{
		timeFunc:  timeFunc,
		sinceFunc: sinceFunc,

		opTime:    newTimer(meter, "oxia_client_op"),
		opPayload: newHistogram(meter, "oxia_client_op_payload", unit.Bytes),

		batchTotalTime: newTimer(meter, "oxia_client_batch_total"),
		batchExecTime:  newTimer(meter, "oxia_client_batch_exec"),
		batchPayload:   newHistogram(meter, "oxia_client_batch_payload", unit.Bytes),
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
		m.opPayload.Record(ctx, int64(len(put.Payload)), _attrs...)
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
			size = int64(len(response.Payload))
		}
		m.opPayload.Record(ctx, size, _attrs...)
	}
	return get
}

func (m *Metrics) DecorateGetRange(getRange model.GetRangeCall) model.GetRangeCall {
	callback := getRange.Callback
	metricContext := m.metricContextFunc("get_range")
	getRange.Callback = func(response *proto.GetRangeResponse, err error) {
		callback(response, err)
		ctx, start, _attrs := metricContext(err)
		m.opTime.Record(ctx, m.sinceFunc(start), _attrs...)
	}
	return getRange
}

func (m *Metrics) WriteCallback() func(time.Time, *proto.WriteRequest, *proto.WriteResponse, error) {
	metricContext := m.metricContextFunc("write")
	return func(executionStart time.Time, request *proto.WriteRequest, response *proto.WriteResponse, err error) {
		ctx, batchStart, _attrs := metricContext(err)
		m.batchTotalTime.Record(ctx, m.sinceFunc(batchStart), _attrs...)
		m.batchExecTime.Record(ctx, m.sinceFunc(executionStart), _attrs...)
		payloadSize, requestCount := writeMetrics(request)
		m.batchPayload.Record(ctx, payloadSize, _attrs...)
		m.batchRequests.Record(ctx, requestCount, _attrs...)
	}
}

func (m *Metrics) ReadCallback() func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error) {
	metricContext := m.metricContextFunc("read")
	return func(executionStart time.Time, request *proto.ReadRequest, response *proto.ReadResponse, err error) {
		ctx, batchStart, attrs := metricContext(err)
		m.batchTotalTime.Record(ctx, m.sinceFunc(batchStart), attrs...)
		m.batchExecTime.Record(ctx, m.sinceFunc(executionStart), attrs...)
		payloadSize, requestCount := readMetrics(response)
		m.batchPayload.Record(ctx, payloadSize, attrs...)
		m.batchRequests.Record(ctx, requestCount, attrs...)
	}
}

func (m *Metrics) metricContextFunc(requestType string) func(error) (context.Context, time.Time, []attribute.KeyValue) {
	start := m.timeFunc()
	return func(err error) (context.Context, time.Time, []attribute.KeyValue) {
		return context.TODO(), start, attrs(requestType, err)
	}
}
