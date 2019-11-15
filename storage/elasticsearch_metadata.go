package storage

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)
const (
 namespace                            = "elasticsearch"
 metrics_metadata_index               = "biggraphite_metrics"
 metrics_metadata_index_suffix_format = "_2006-01-02"
 mapping                              = `
{
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
`
)

type BgMetadataElasticSearchConnector struct {
	client ElasticSearchClient
	UpdatedMetrics  *prometheus.CounterVec
	WriteDuration prometheus.Histogram
	KnownIndices map[string]bool
}

type ElasticSearchClient interface {
	Perform(*http.Request) (*http.Response, error)
}

func NewBgMetadataElasticSearchConnector(elasticSearchClient ElasticSearchClient, registry prometheus.Registerer) *BgMetadataElasticSearchConnector {
	var esc = BgMetadataElasticSearchConnector{
		client: elasticSearchClient,
		UpdatedMetrics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "updated_metrics",
			Help:      "total number of metrics updated in ElasticSearch",
		}, []string{"status"}),
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "write_duration_ms",
		Help:      "time spent writing to ElasticSearch",
		Buckets:   []float64{1000, 3000, 5000, 6000, 10000, 15000, 20000, 50000, 100000}}),
	}
	_ = registry.Register(esc.UpdatedMetrics)
	_ = registry.Register(esc.WriteDuration)

	esc.KnownIndices = map[string]bool{}

	return &esc
}

func NewBgMetadataElasticSearchConnectorWithDefaults(server string) *BgMetadataElasticSearchConnector {
	cfg := elasticsearch.Config{
		Addresses: []string{
			server,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the ElasticSearch client: %s", err)
	}

	_, err = es.Info()
	if err != nil {
		log.Fatalf("Error getting ElasticSearch information response: %s", err)
	}

	return NewBgMetadataElasticSearchConnector(es, prometheus.DefaultRegisterer)
}

func (esc *BgMetadataElasticSearchConnector) createIndexAndMapping(indexName string) error {
	indexCreateRequest := esapi.IndicesCreateRequest{Index: indexName}
	res, err := indexCreateRequest.Do(context.Background(), esc.client)


	r := strings.NewReader(mapping)
	request := esapi.IndicesPutMappingRequest{Index: []string{indexName}, Body: r}
	res, err = request.Do(context.Background(), esc.client)

	if err != nil {
		return fmt.Errorf("Could not set ElasticSearch mapping: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("Could not set ElasticSearch mapping (status %d)", res.StatusCode)
	}

	x, _ := ioutil.ReadAll(res.Body)
	fmt.Printf(string(x))

	return nil
}

func (esc *BgMetadataElasticSearchConnector) UpdateMetricMetadata(metric Metric) error {
	indexName, err := esc.getIndex()
	if err != nil {
		esc.UpdatedMetrics.WithLabelValues("failure").Inc()
		return fmt.Errorf("Could not get index for metric %s: %w", metric.name, err)
	}

	req := esapi.IndexRequest{
		Index: indexName,
		Body:  strings.NewReader(BuildElasticSearchDocument(metric)),
		DocumentID: metric.id,
	}
	timeBeforeWrite := time.Now()
	res, err := req.Do(context.Background(), esc.client)
	esc.WriteDuration.Observe(float64(time.Since(timeBeforeWrite).Microseconds()))

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
	defer res.Body.Close()
	esc.UpdatedMetrics.WithLabelValues("success").Inc()

	return nil
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
