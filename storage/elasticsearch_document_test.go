package storage

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDocumentIsCorrect(t *testing.T) {
	metadata := MetricMetadata{aggregator: "<a>", carbonXfilesfactor: "<c>", retention: "<r>"}
	metric := NewMetric("a.b.c", metadata)
	doc := BuildElasticSearchDocument(metric)

	var jsonMap map[string]interface{}
	_ = json.Unmarshal([]byte(doc), &jsonMap)

	assert.Equal(t, jsonMap["name"], "a.b.c")
	assert.Equal(t, jsonMap["p0"], "a")
	assert.Equal(t, jsonMap["p1"], "b")
	assert.Equal(t, jsonMap["p2"], "c")
	assert.Equal(t, jsonMap["depth"], "2")
	configMap := jsonMap["config"].(map[string]interface{})
	assert.Equal(t, configMap["carbon_xfilesfactor"], "<c>")
}
