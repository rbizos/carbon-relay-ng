package storage

import (
	"github.com/graphite-ng/carbon-relay-ng/storage/mocks"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

func TestWritesAMetricMetadata(t *testing.T) {
	mockElasticSearchClient := &mocks.ElasticSearchClient{}
	registry := prometheus.NewRegistry()
	esc := NewBgMetadataElasticSearchConnector(mockElasticSearchClient, registry)

	response := http.Response{Body: ioutil.NopCloser(strings.NewReader("<response>"))}
	mockElasticSearchClient.On("Perform", mock.Anything).Return(&response, nil)

	err := esc.UpdateMetricMetadata(createMetric())

	assert.Nil(t, err)
	successes := getMetricValue(esc.UpdatedMetrics, map[string]string{"status": "success"})
	assert.Equal(t, successes, 1.0)
	mockElasticSearchClient.AssertExpectations(t)
}

func TestHandlesFailureWhenWritingAMetricMetadata(t *testing.T) {
	mockElasticSearchClient := &mocks.ElasticSearchClient{}
	registry := prometheus.NewRegistry()
	esc := NewBgMetadataElasticSearchConnector(mockElasticSearchClient, registry)

	mockElasticSearchClient.On("Perform", mock.Anything).Return(&http.Response{}, errors.New("<error>"))

	err := esc.UpdateMetricMetadata(createMetric())

	assert.NotNil(t, err)
	failures := getMetricValue(esc.UpdatedMetrics, map[string]string{"status": "failure"})
	assert.Equal(t, failures, 1.0)
	mockElasticSearchClient.AssertExpectations(t)
}

func getMetricValue(counterVec *prometheus.CounterVec, labels prometheus.Labels) float64 {
	counter, _ := counterVec.GetMetricWith(labels)
	metric := &dto.Metric{}
	_ = counter.Write(metric)
	return *metric.Counter.Value
}

func createMetric() Metric {
	metadata := MetricMetadata{
		aggregator: "",
		carbonXfilesfactor: "",
		retention: "",
	}
	return NewMetric("a.b.c", metadata)
}