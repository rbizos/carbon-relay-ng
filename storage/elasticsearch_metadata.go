package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/graphite-ng/carbon-relay-ng/cfg"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	namespace                            = "elasticsearch"
	default_metrics_metadata_index       = "biggraphite_metrics"
	metrics_metadata_index_suffix_format = "_2006-01-02"
	directories_index_suffix             = "_directories"
	metricsMapping                       = `
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
	dirMapping = `
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
            "parent": {
                "type": "keyword"
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
	client                  ElasticSearchClient
	UpdatedDocuments        *prometheus.CounterVec // TODO split into updated metrics and updated directories
	HTTPErrors              *prometheus.CounterVec
	WriteDurationMs         prometheus.Histogram
	DocumentBuildDurationMs prometheus.Histogram
	KnownIndices            map[string]bool
	BulkBuffer              []ElasticSearchDocument
	BulkSize                uint
	Mux                     sync.Mutex
	MaxRetry                uint
	IndexName, currentIndex string
	logger                  *zap.Logger
}

type ElasticSearchClient interface {
	Perform(*http.Request) (*http.Response, error)
}

// NewBgMetadataElasticSearchConnector : contructor for BgMetadataElasticSearchConnector
func newBgMetadataElasticSearchConnector(elasticSearchClient ElasticSearchClient, registry prometheus.Registerer, bulkSize, maxRetry uint, indexName string) *BgMetadataElasticSearchConnector {
	var esc = BgMetadataElasticSearchConnector{
		client:     elasticSearchClient,
		BulkSize:   bulkSize,
		BulkBuffer: make([]ElasticSearchDocument, 0, bulkSize),
		MaxRetry:   maxRetry,
		IndexName:  indexName,

		UpdatedDocuments: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "updated_documents",
			Help:      "total number of documents updated in ElasticSearch partitionned between metrics and directories",
		}, []string{"status", "type"}),

		HTTPErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_errors",
			Help:      "total number of http errors encountered partitionned by status code",
		}, []string{"code"}),

		WriteDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "write_duration_ms",
			Help:      "time spent writing to ElasticSearch based on `took` field of response ",
			Buckets:   []float64{250, 500, 750, 1000, 1500, 2000, 5000, 10000}}),
		DocumentBuildDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "document_build_duration_ms",
			Help:      "time spent building an ElasticSearch document",
			Buckets:   []float64{1, 5, 10, 50, 100, 250, 500, 750, 1000, 2000}}),
		logger: zap.L(),
	}
	_ = registry.Register(esc.UpdatedDocuments)
	_ = registry.Register(esc.WriteDurationMs)
	_ = registry.Register(esc.DocumentBuildDurationMs)
	if esc.IndexName == "" {
		esc.IndexName = default_metrics_metadata_index
	}

	esc.KnownIndices = map[string]bool{}
	return &esc
}

func createElasticSearchClient(server, username, password string) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			server,
		},
		Username: username,
		Password: password,
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

// NewBgMetadataElasticSearchConnectorWithDefaults is the public contructor of BgMetadataElasticSearchConnector
func NewBgMetadataElasticSearchConnectorWithDefaults(cfg *cfg.BgMetadataESConfig) *BgMetadataElasticSearchConnector {
	es, err := createElasticSearchClient(cfg.StorageServer, cfg.Username, cfg.Password)

	if err != nil {
		log.Fatalf("Could not create ElasticSearch connector: %w", err)
	}

	return newBgMetadataElasticSearchConnector(es, prometheus.DefaultRegisterer, cfg.BulkSize, cfg.MaxRetry, cfg.IndexName)
}

func (esc *BgMetadataElasticSearchConnector) Close() {
}

func (esc *BgMetadataElasticSearchConnector) createIndicesAndMapping(metricIndexName, directoryIndexName string) error {
	indices := []struct{ name, mapping string }{{metricIndexName, metricsMapping}, {directoryIndexName, dirMapping}}
	for _, index := range indices {
		indexCreateRequest := esapi.IndicesCreateRequest{Index: index.name}
		res, err := indexCreateRequest.Do(context.Background(), esc.client)

		// extract TODO error deserialize
		r := strings.NewReader(index.mapping)
		request := esapi.IndicesPutMappingRequest{Index: []string{index.name}, Body: r, DocumentType: documentType}
		res, err = request.Do(context.Background(), esc.client)

		if err != nil {
			return fmt.Errorf("Could not set ElasticSearch mapping: %w", err)
		}
		if res.StatusCode != http.StatusOK {
			errorMessage, _ := ioutil.ReadAll(res.Body)
			return fmt.Errorf("Could not set ElasticSearch mapping (status %d, error: %s)", res.StatusCode, errorMessage)
		}
	}
	return nil
}

// UpdateMetricMetadata stores the metric in a buffer, will bulkupdate when at full cap
// threadsafe
func (esc *BgMetadataElasticSearchConnector) UpdateMetricMetadata(metric *Metric) error {
	return esc.addDocumentToBuff(metric)
}

func (esc *BgMetadataElasticSearchConnector) addDocumentToBuff(doc ElasticSearchDocument) error {
	esc.Mux.Lock()
	defer esc.Mux.Unlock()

	esc.BulkBuffer = append(esc.BulkBuffer, doc)
	if len(esc.BulkBuffer) == cap(esc.BulkBuffer) {
		esc.sendAndClearBuffer()
	}
	return nil
}

func (esc *BgMetadataElasticSearchConnector) sendAndClearBuffer() error {
	defer esc.clearBuffer()
	metricIndex, directoryIndex, err := esc.getIndices()
	var errorMessage []byte
	var statusCode int

	if err != nil {
		esc.UpdatedDocuments.WithLabelValues("failure").Add(float64(len(esc.BulkBuffer)))
		return fmt.Errorf("Could not get index: %w", err)
	}

	timeBeforeBuild := time.Now()
	requestBody := BuildElasticSearchDocumentMulti(metricIndex, directoryIndex, esc.BulkBuffer)
	esc.DocumentBuildDurationMs.Observe(float64(time.Since(timeBeforeBuild).Milliseconds()))

	for attempt := uint(0); attempt <= esc.MaxRetry; attempt++ {
		res, err := esc.bulkUpdate(requestBody)

		if err != nil {
			// esapi resturns a nil body in case of error
			esc.UpdatedDocuments.WithLabelValues("failure", "any").Add(float64(len(esc.BulkBuffer)))
			return fmt.Errorf("Could not write to index: %w", err)
		}

		if !res.IsError() {
			esc.updateInternalMetrics(res)
			res.Body.Close()
			return nil

		} else {
			esc.HTTPErrors.WithLabelValues(strconv.Itoa(res.StatusCode)).Inc()
			statusCode = res.StatusCode
			errorMessage, _ = ioutil.ReadAll(res.Body)
			res.Body.Close()
		}
	}

	esc.UpdatedDocuments.WithLabelValues("failure", "any").Add(float64(len(esc.BulkBuffer)))
	return fmt.Errorf("Could not write to index (status %d, error: %s)", statusCode, errorMessage)

}

// updateInternalMetrics increments BGMetadataConnector's metrics,
func (esc *BgMetadataElasticSearchConnector) updateInternalMetrics(res *esapi.Response) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			esc.logger.Warn("malformed bulk response", zap.Error(err.(error)))
		}
	}()
	var mapResp map[string]interface{}
	json.NewDecoder(res.Body).Decode(&mapResp)
	esc.WriteDurationMs.Observe(mapResp["took"].(float64))
	for _, item := range mapResp["items"].([]interface{}) {
		mapCreate := item.(map[string]interface{})["create"].(map[string]interface{})
		// protected by esc.Mux currentIndex may not change while looping
		if int(mapCreate["status"].(float64)) == http.StatusCreated {
			if mapCreate["_index"] == esc.currentIndex {
				esc.UpdatedDocuments.WithLabelValues("created", "metric").Inc()
			} else {
				esc.UpdatedDocuments.WithLabelValues("created", "directory").Inc()
			}
		}
	}
}

func (esc *BgMetadataElasticSearchConnector) clearBuffer() error {
	esc.BulkBuffer = esc.BulkBuffer[:0]
	return nil
}

func (esc *BgMetadataElasticSearchConnector) bulkUpdate(body string) (*esapi.Response, error) {

	req := esapi.BulkRequest{
		Body:         strings.NewReader(body),
		DocumentType: documentType,
	}

	res, err := req.Do(context.Background(), esc.client)
	return res, err
}

func (esc *BgMetadataElasticSearchConnector) getIndices() (string, string, error) {
	metricIndexName, directoryIndexName := getIndicesNames(esc.IndexName)
	esc.currentIndex = metricIndexName
	_, isKnownIndex := esc.KnownIndices[metricIndexName]

	// no need to test both
	if !isKnownIndex {
		err := esc.createIndicesAndMapping(metricIndexName, directoryIndexName)
		if err != nil {
			return "", "", err
		}
		esc.KnownIndices[metricIndexName] = true
	}

	return metricIndexName, directoryIndexName, nil
}

// InsertDirectory will add directory to the bulkBuffer
func (esc *BgMetadataElasticSearchConnector) InsertDirectory(dir *MetricDirectory) error {
	esc.addDocumentToBuff(dir)
	return nil
}

// SelectDirectory unused, no need in ES
// returns an error to signal that parent dir does not exist
func (esc *BgMetadataElasticSearchConnector) SelectDirectory(dir string) (string, error) {
	return dir, fmt.Errorf("")
}

func getIndicesNames(baseName string) (metricIndexName, directoryIndexName string) {
	now := time.Now().Format(metrics_metadata_index_suffix_format)
	metricIndexName = baseName + now
	directoryIndexName = baseName + directories_index_suffix + now
	return
}
