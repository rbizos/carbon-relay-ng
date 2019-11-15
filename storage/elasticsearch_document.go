package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func BuildElasticSearchDocument(metric Metric) string {
	var components = strings.Split(metric.name, ".")
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`{"name":"%s",`, metric.name))
	b.WriteString(fmt.Sprintf(`"depth": "%d",`, len(components) - 1))
	b.WriteString(fmt.Sprintf(`"uuid": "%s",`, metric.id))


	//	b.WriteString(fmt.Sprintf(`"created_on":"%s",`, metric.createdOn))
	//	b.WriteString(fmt.Sprintf(`"updated_on":"%s",`, metric.updatedOn))
	//	b.WriteString(fmt.Sprintf(`"read_on": "%s",`, metric.readOn))
	// TODO: remove this patch.
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000000")
	b.WriteString(fmt.Sprintf(`"created_on":"%s",`, now))
	b.WriteString(fmt.Sprintf(`"updated_on":"%s",`, now))
	b.WriteString(fmt.Sprintf(`"read_on": null,`))
	metric.config["retention"] = "10080*60s"
	metric.config["aggregator"] = "average"

	configSerialized, _ := json.Marshal(metric.config)
	b.WriteString(fmt.Sprintf(`"config": %s`, configSerialized))

	for i, component := range components {
		b.WriteString(",")
		b.WriteString(fmt.Sprintf(`"p%d":"%s"`, i, component))
	}
	b.WriteString(`}`)
	return b.String()
}
