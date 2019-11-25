package storage

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)
const (
 namespace                            = "elasticsearch"
 metrics_metadata_index               = "biggraphite_metrics"
 metrics_metadata_index_suffix_format = "_2006-01-02"
 mapping                              = `
{
"_doc": {
"properties": {
            "depth": { 
                "type": "long"
            },

            "name": {
                "type": "keyword",
                "ignore_above": 1024
            },
            "uuid": {
                "type": "keyword"
            },
            "config": {
                "type": "object"
            }
        },
			"dynamic_templates": [
            {
                "strings_as_keywords": {
                    "match": "p*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword",
                        "ignore_above": 256,
                        "ignore_malformed": true
                    }
                }
            }
        ]
	}
}
`
 documentType = "_doc"
)

type BgMetadataElasticSearchConnector struct {
	client          ElasticSearchClient
	UpdatedMetrics  *prometheus.CounterVec
	WriteDurationMs prometheus.Histogram
	DocumentBuildDurationMs prometheus.Histogram
	KnownIndices    map[string]bool
	InputChannel    chan GroupingChannelElement
	GroupingChannel *GroupingChannel
	BulkSize        uint
	WaitGroup       *sync.WaitGroup
}

type ElasticSearchClient interface {
	Perform(*http.Request) (*http.Response, error)
}

func NewBgMetadataElasticSearchConnector(elasticSearchClient ElasticSearchClient, registry prometheus.Registerer, bulkSize uint) *BgMetadataElasticSearchConnector {
	var esc = BgMetadataElasticSearchConnector{
		client: elasticSearchClient,
		UpdatedMetrics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "updated_metrics",
			Help:      "total number of metrics updated in ElasticSearch",
		}, []string{"status"}),
		WriteDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "write_duration_ms",
		Help:      "time spent writing to ElasticSearch",
		Buckets:   []float64{250, 500, 750, 1000, 1500, 2000, 5000, 10000}}),
		DocumentBuildDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "document_build_duration_ms",
			Help:      "time spent building an ElasticSearch document",
			Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 750, 1000, 2000}}),
	}
	_ = registry.Register(esc.UpdatedMetrics)
	_ = registry.Register(esc.WriteDurationMs)
	_ = registry.Register(esc.DocumentBuildDurationMs)

	esc.KnownIndices = map[string]bool{}

	esc.InputChannel = make(chan GroupingChannelElement, bulkSize * 2)
	var wg sync.WaitGroup
	esc.WaitGroup = &wg
	esc.WaitGroup.Add(1)
	esc.GroupingChannel = NewGroupingChannel(bulkSize, esc.InputChannel, esc.WaitGroup)

	esc.WaitGroup.Add(1)
	go esc.RunWriter(esc.WaitGroup)

	return &esc
}

func CreateElasticSearchClient(server string) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			server,
		},
	}

	username, exists := os.LookupEnv("ES_USER")
	if exists {
		cfg.Username = username
	}
	password, exists := os.LookupEnv("ES_PASS")
	if exists {
		cfg.Password = password
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the ElasticSearch client: %s", err)
	}

	_, err = es.Info()
	if err != nil {
		log.Fatalf("Error getting ElasticSearch information response: %s", err)
	}

	return es, err
}

func NewBgMetadataElasticSearchConnectorWithDefaults(server string, bulkSize uint) *BgMetadataElasticSearchConnector {
	es, err := CreateElasticSearchClient(server)

	if err != nil {
		log.Fatalf("Could not create ElasticSearch connector: %w", err)
	}

	return NewBgMetadataElasticSearchConnector(es, prometheus.DefaultRegisterer, bulkSize)
}

func (esc *BgMetadataElasticSearchConnector) Close()  {
	close(esc.InputChannel)
	esc.WaitGroup.Wait()
}

func (esc *BgMetadataElasticSearchConnector) createIndexAndMapping(indexName string) error {
	indexCreateRequest := esapi.IndicesCreateRequest{Index: indexName}
	res, err := indexCreateRequest.Do(context.Background(), esc.client)

	// extract TODO error deserialize
	r := strings.NewReader(mapping)
	request := esapi.IndicesPutMappingRequest{Index: []string{indexName}, Body: r, DocumentType: documentType}
	res, err = request.Do(context.Background(), esc.client)

	if err != nil {
		return fmt.Errorf("Could not set ElasticSearch mapping: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		errorMessage, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Could not set ElasticSearch mapping (status %d, error: %s)", res.StatusCode, errorMessage)
	}

	return nil
}

func (esc *BgMetadataElasticSearchConnector) UpdateMetricMetadata(metric Metric) error {
	esc.InputChannel <- metric
	return nil
}

func (esc *BgMetadataElasticSearchConnector) RunWriter(wg *sync.WaitGroup) error {
	defer wg.Done()
	metrics := make([]Metric, 0)

	for metricGroup := range esc.GroupingChannel.groupedChannel {
		for _, metricChannelElement := range metricGroup {
			metrics = append(metrics, metricChannelElement.(Metric))
		}
		err := esc.UpdateMetricMetadataMulti(metrics)
		metrics = metrics[:0]
		if err != nil {
			return fmt.Errorf("Could not write metadata metrics group: %w", err)
		}
	}

	return nil
}

func (esc *BgMetadataElasticSearchConnector) UpdateMetricMetadataMulti(metrics []Metric) error {
	indexName, err := esc.getIndex()
	if err != nil {
		esc.UpdatedMetrics.WithLabelValues("failure").Add(float64(len(metrics)))
		return fmt.Errorf("Could not get index: %w", err)
	}

	res, err := esc.WriteToIndexMulti(indexName, metrics)
	defer res.Body.Close()

	if err != nil {
		esc.UpdatedMetrics.WithLabelValues("failure").Add(float64(len(metrics)))
		return fmt.Errorf("Could not write to index: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		esc.UpdatedMetrics.WithLabelValues("failure").Add(float64(len(metrics)))
		errorMessage, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Could not write to index (status %d, error: %s)", res.StatusCode, errorMessage)
	}

	esc.UpdatedMetrics.WithLabelValues("success").Add(float64(len(metrics)))

	return nil
}

func (esc *BgMetadataElasticSearchConnector) WriteToIndexMulti(indexName string, metrics []Metric) (*esapi.Response, error) {
	timeBeforeBuild := time.Now()
	doc := BuildElasticSearchDocumentMulti(indexName, metrics)
	esc.DocumentBuildDurationMs.Observe(float64(time.Since(timeBeforeBuild).Milliseconds()))

	req := esapi.BulkRequest{
		Index:        indexName,
		Body:         strings.NewReader(doc),
		DocumentType: documentType,
	}

	timeBeforeWrite := time.Now()
	res, err := req.Do(context.Background(), esc.client)
	esc.WriteDurationMs.Observe(float64(time.Since(timeBeforeWrite).Milliseconds()))

	return res, err
}

func (esc *BgMetadataElasticSearchConnector) UpdateMetricMetadataSingle(metric Metric) error {
	indexName, err := esc.getIndex()
	if err != nil {
		esc.UpdatedMetrics.WithLabelValues("failure").Inc()
		return fmt.Errorf("Could not get index for metric %s: %w", metric.name, err)
	}

	res, err := esc.WriteToIndex(indexName, metric)
	defer res.Body.Close()


	if err != nil {
		esc.UpdatedMetrics.WithLabelValues("failure").Inc()
		log.Printf("Error getting response: %s", err) // TODO: how should this error be handled?
		return err
	}
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		esc.UpdatedMetrics.WithLabelValues("failure").Inc()
		errorMessage, _ := ioutil.ReadAll(res.Body)
		err = fmt.Errorf("Insertion failed in ElasticSearch (StatusCode: %d): %s", res.StatusCode, string(errorMessage))
		log.Printf("Error getting response: %s", err) // TODO: how should this error be handled?
		return err
	}
	esc.UpdatedMetrics.WithLabelValues("success").Inc()

	return nil
}

func (esc *BgMetadataElasticSearchConnector) WriteToIndex(indexName string, metric Metric) (*esapi.Response, error) {
	req := esapi.IndexRequest{
		Index:      indexName,
		Body:       strings.NewReader(BuildElasticSearchDocument(metric)),
		DocumentID: metric.id,
	}
	return req.Do(context.Background(), esc.client)
}

func (esc *BgMetadataElasticSearchConnector) getIndex() (string, error) {
	indexName := metrics_metadata_index + time.Now().Format(metrics_metadata_index_suffix_format)

	_, isKnownIndex := esc.KnownIndices[indexName]
	if !isKnownIndex {
		err := esc.createIndexAndMapping(indexName)
		if err != nil {
			return "", err
		}
		esc.KnownIndices[indexName] = true
	}

	return indexName, nil
}

func (esc *BgMetadataElasticSearchConnector) InsertDirectory(dir MetricDirectory) error {
	return nil
}

func (esc *BgMetadataElasticSearchConnector) SelectDirectory(dir string) (string, error) {
	return "", nil
}
