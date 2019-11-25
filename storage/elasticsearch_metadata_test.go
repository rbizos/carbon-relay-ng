package storage

import (
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/graphite-ng/carbon-relay-ng/storage/mocks"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"testing"
)

func TestWritesAMetricMetadata(t *testing.T) {
	mockElasticSearchClient := &mocks.ElasticSearchClient{}
	registry := prometheus.NewRegistry()
	esc := NewBgMetadataElasticSearchConnector(mockElasticSearchClient, registry, 10)

	response := http.Response{Body: ioutil.NopCloser(strings.NewReader("<response>")), StatusCode: 200}
	mockElasticSearchClient.On("Perform", mock.Anything).Return(&response, nil)

	err := esc.UpdateMetricMetadata(createMetric())
	esc.Close()

	assert.Nil(t, err)
	successes := getMetricValue(esc.UpdatedMetrics, map[string]string{"status": "success"})
	assert.Equal(t, successes, 1.0)
	mockElasticSearchClient.AssertExpectations(t)
}

func TestHandlesFailureWhenWritingAMetricMetadata(t *testing.T) {
	mockElasticSearchClient := &mocks.ElasticSearchClient{}
	registry := prometheus.NewRegistry()
	esc := NewBgMetadataElasticSearchConnector(mockElasticSearchClient, registry, 10)

	mockElasticSearchClient.On("Perform", mock.Anything).Return(&http.Response{}, errors.New("<error>"))

	err := esc.UpdateMetricMetadata(createMetric())
	esc.Close()

	assert.Nil(t, err)
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

func createMetricWithName(name string) Metric {
	metadata := MetricMetadata{
		aggregator: "",
		carbonXfilesfactor: "",
		retention: "",
	}
	return NewMetric(name, metadata)
}

func createMetric() Metric {
	return createMetricWithName("a.b.c")
}

func createRandomMetric() Metric {
	length := 10
	var b strings.Builder
	for i := 0; i < length - 1; i++ {
		b.WriteString(fmt.Sprintf("%d.", rand.Uint32()))
	}
	b.WriteString(fmt.Sprintf("%d", rand.Uint32()))

	return createMetricWithName(b.String())
}

func WriteRandomMetrics(numMetrics int, esc *BgMetadataElasticSearchConnector) {
	var err error
	for i := 0; i < numMetrics; i++ {
		err = esc.UpdateMetricMetadata(createRandomMetric())
		if err != nil {
			log.Fatal(err)
		}
	}
}

func benchmarkWrites(b *testing.B, esc *BgMetadataElasticSearchConnector, numMetrics int) {
	WriteRandomMetrics(numMetrics, esc)
}
const numMetrics = 10000
var es *elasticsearch.Client = nil

func BenchmarkWritesWithBulkSize100(b *testing.B) {
	fmt.Printf("bb %d\n", b.N)
	rand.Seed(0)
	es, err := GetClient()
	if err != nil {
		b.Fail()
	}
	esc := NewBgMetadataElasticSearchConnector(es, prometheus.DefaultRegisterer, 100)
	for n := 0; n < b.N; n++ {
		benchmarkWrites(b, esc, numMetrics)
	}
}

func GetClient() (*elasticsearch.Client, error) {
	if es == nil {
		fmt.Printf("creating\n")
		es, err := CreateElasticSearchClient("http://localhost:9200")
		return es, err
	}
	fmt.Printf("cache\n")

	return es, nil
}
