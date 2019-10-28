package storage

import "gopkg.in/ini.v1"

type StorageAggregation struct {
	ID                string
	pattern           string
	xFilesFactor      string
	aggregationMethod string
}

func NewStorageAggregations(storageAggregationConf string) ([]StorageAggregation, error) {
	aggr := []StorageAggregation{}
	cfg, err := ini.Load(storageAggregationConf)
	if err != nil {
		return aggr, nil
	}
	for _, section := range cfg.Sections()[1:] { // first element is empty default value
		aggr = append(
			aggr,
			StorageAggregation{
				ID:                section.Name(),
				pattern:           section.KeysHash()["PATTERN"],
				xFilesFactor:      section.KeysHash()["XFILESFACTOR"],
				aggregationMethod: section.KeysHash()["AGGREGATIONMETHOD"],
			},
		)
	}
	return aggr, nil
}
