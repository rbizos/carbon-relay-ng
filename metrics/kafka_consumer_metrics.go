package metrics

import (
	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
)

type KafkaConsumerMetrics struct {
	ID          string
	kafkaConfig *sarama.Config
}

func RegisterKafkaConsumerMetrics(id string, kafkaConfig *sarama.Config) error {
	m := &KafkaConsumerMetrics{
		id, kafkaConfig,
	}
	return prometheus.Register(m)
}

func (k *KafkaConsumerMetrics) Describe(chan<- *prometheus.Desc) {
	return
}
func (k *KafkaConsumerMetrics) Collect(c chan<- prometheus.Metric) {
	metrics := k.kafkaConfig.MetricRegistry.GetAll()
	for key, value := range metrics {
		key = strings.ReplaceAll(key, "-", "_") // Because prometheus does not handle - in metric
		if value["count"] != nil {              // counter see MetricRegistry.GetAll() implementation
			value := value["count"].(int64)
			counter := prometheus.NewCounter(prometheus.CounterOpts{
				Namespace:   "kafka",
				Subsystem:   "consumer",
				Name:        key,
				Help:        "",
				ConstLabels: nil,
			})
			counter.Add(float64(value))
			c <- counter
		} else if value["value"] != nil { // gauge see MetricRegistry.GetAll() implementation
			gauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "kafka",
				Subsystem:   "consumer",
				Name:        key,
				Help:        "",
				ConstLabels: nil,
			})
			gauge.Set(value["value"].(float64))
			c <- gauge
		}

	}
}
