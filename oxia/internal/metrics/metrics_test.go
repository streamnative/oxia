package metrics

import (
	"context"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"io"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"testing"
	"time"
)

func TestMetricsDecorate(t *testing.T) {
	putFunc := func(metrics *Metrics, err error) {
		metrics.DecoratePut(model.PutCall{Callback: func(*proto.PutResponse, error) {}, Payload: []byte{0, 1, 2, 3, 4}}).
			Callback(&proto.PutResponse{}, err)
	}
	deleteFunc := func(metrics *Metrics, err error) {
		metrics.DecorateDelete(model.DeleteCall{Callback: func(*proto.DeleteResponse, error) {}}).
			Callback(&proto.DeleteResponse{}, err)
	}
	deleteRangeFunc := func(metrics *Metrics, err error) {
		metrics.DecorateDeleteRange(model.DeleteRangeCall{Callback: func(*proto.DeleteRangeResponse, error) {}}).
			Callback(&proto.DeleteRangeResponse{}, err)
	}
	getFunc := func(metrics *Metrics, err error) {
		metrics.DecorateGet(model.GetCall{Callback: func(*proto.GetResponse, error) {}}).
			Callback(&proto.GetResponse{Payload: []byte{0, 1, 2, 3, 4}}, err)
	}
	getRangeFunc := func(metrics *Metrics, err error) {
		metrics.DecorateGetRange(model.GetRangeCall{Callback: func(*proto.GetRangeResponse, error) {}}).
			Callback(&proto.GetRangeResponse{}, err)
	}

	for _, item := range []struct {
		testFunc     func(*Metrics, error)
		expectedType string
		hasHistogram bool
	}{
		{putFunc, "put", true},
		{deleteFunc, "delete", false},
		{deleteRangeFunc, "delete_range", false},
		{getFunc, "get", true},
		{getRangeFunc, "get_range", false},
	} {
		for _, condition := range []struct {
			err            error
			expectedResult string
		}{
			{nil, "success"},
			{io.EOF, "failure"},
		} {
			t.Run(item.expectedType+"_"+condition.expectedResult, func(t *testing.T) {
				ch := make(chan time.Duration, 1)
				ch <- 1 * time.Millisecond
				metrics, reader := setup(func(t time.Time) time.Duration {
					return <-ch
				})

				item.testFunc(metrics, condition.err)

				rm, err := reader.Collect(context.Background())
				assert.NoError(t, err)

				assertTimer(t, rm, "oxia_client_op", item.expectedType, condition.expectedResult)
				assertHistogram(t, rm, "oxia_client_op_payload", item.hasHistogram, float64(5), item.expectedType, condition.expectedResult)
			})
		}
	}
}

func TestMetricsCallback(t *testing.T) {
	writeFunc := func(metrics *Metrics, err error) {
		metrics.WriteCallback()(time.Now(), &proto.WriteRequest{
			Puts: []*proto.PutRequest{{Payload: []byte{0, 1, 2, 3, 4}}},
		}, &proto.WriteResponse{}, err)
	}
	readFunc := func(metrics *Metrics, err error) {
		metrics.ReadCallback()(time.Now(), &proto.ReadRequest{}, &proto.ReadResponse{
			Gets: []*proto.GetResponse{{Payload: []byte{0, 1, 2, 3, 4}}},
		}, err)
	}

	for _, item := range []struct {
		testFunc     func(*Metrics, error)
		expectedType string
	}{
		{writeFunc, "write"},
		{readFunc, "read"},
	} {
		for _, condition := range []struct {
			err            error
			expectedResult string
		}{
			{nil, "success"},
			{io.EOF, "failure"},
		} {
			t.Run(item.expectedType+"_"+condition.expectedResult, func(t *testing.T) {
				ch := make(chan time.Duration, 2)
				ch <- 1 * time.Millisecond
				ch <- 1 * time.Millisecond
				metrics, reader := setup(func(t time.Time) time.Duration {
					return <-ch
				})

				item.testFunc(metrics, condition.err)

				rm, err := reader.Collect(context.Background())
				assert.NoError(t, err)

				assertTimer(t, rm, "oxia_client_batch_total", item.expectedType, condition.expectedResult)
				assertTimer(t, rm, "oxia_client_batch_exec", item.expectedType, condition.expectedResult)
				assertHistogram(t, rm, "oxia_client_batch_payload", true, float64(5), item.expectedType, condition.expectedResult)
				assertHistogram(t, rm, "oxia_client_batch_request", true, float64(1), item.expectedType, condition.expectedResult)
			})
		}
	}
}

func setup(sinceFunc func(time.Time) time.Duration) (*Metrics, metric.Reader) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	metrics := newMetrics(provider, time.Now, sinceFunc)
	return metrics, reader
}

func assertTimer(t *testing.T, rm metricdata.ResourceMetrics, name string, expectedType string, expectedResult string) {
	sums, err := counter[float64](rm, name)
	assert.NoError(t, err)
	assertDataPoints(t, sums, float64(1), expectedType, expectedResult)

	counts, err := counter[int64](rm, name)
	assert.NoError(t, err)
	assertDataPoints(t, counts, int64(1), expectedType, expectedResult)

}
func assertHistogram(t *testing.T, rm metricdata.ResourceMetrics, name string, hasHistogram bool, expectedSum float64, expectedType string, expectedResult string) {
	datapoints, err := histogram(rm, name)
	assert.NoError(t, err)
	if hasHistogram {
		assert.Equal(t, 1, len(datapoints))
		histogram := datapoints[0]
		assert.Equal(t, expectedSum, histogram.Sum)
		assert.Equal(t, uint64(1), histogram.Count)
		assertAttributes(t, histogram.Attributes, expectedType, expectedResult)
	} else {
		assert.Equal(t, 0, len(datapoints))
	}
}

func counter[N int64 | float64](rm metricdata.ResourceMetrics, name string) ([]metricdata.DataPoint[N], error) {
	for _, m := range rm.ScopeMetrics[0].Metrics {
		if m.Name == name {
			switch d := m.Data.(type) {
			case metricdata.Sum[N]:
				return d.DataPoints, nil
			}
		}
	}
	return nil, errors.New("not found")
}

func histogram(rm metricdata.ResourceMetrics, name string) ([]metricdata.HistogramDataPoint, error) {
	for _, m := range rm.ScopeMetrics[0].Metrics {
		if m.Name == name {
			switch d := m.Data.(type) {
			case metricdata.Histogram:
				return d.DataPoints, nil
			}
		}
	}
	return nil, errors.New("not found")
}

func assertDataPoints[N int64 | float64](t *testing.T, datapoints []metricdata.DataPoint[N], expectedValue N, expectedType string, expectedResult string) {
	assert.Equal(t, 1, len(datapoints))
	assert.Equal(t, expectedValue, datapoints[0].Value)
	assertAttributes(t, datapoints[0].Attributes, expectedType, expectedResult)
}

func assertAttributes(t *testing.T, attrs attribute.Set, expectedType string, expectedResult string) {
	assert.Equal(t, 2, attrs.Len())
	assertAttribute(t, attrs, "type", expectedType)
	assertAttribute(t, attrs, "result", expectedResult)
}
func assertAttribute(t *testing.T, attrs attribute.Set, key attribute.Key, expected string) {
	value, ok := attrs.Value(key)
	assert.True(t, ok)
	assert.Equal(t, expected, value.AsString())
}
