package storage

import (
	"gopkg.in/ini.v1"
	"regexp"
	)

type StorageSchema struct {
	ID         string
	pattern    string
	retentions string
	patternRegex      *regexp.Regexp
}

func NewStorageSchemas(storageSchemasConf string) ([]StorageSchema, error) {
	schemas := []StorageSchema{}
	cfg, err := ini.Load(storageSchemasConf)
	if err != nil {
		return schemas, nil
	}
	for _, section := range cfg.Sections()[1:] { // first element is empty default value
		pattern := section.KeysHash()["PATTERN"]
		patternRegex, _ := regexp.Compile(pattern)
		schemas = append(
			schemas,
			StorageSchema{
				ID:         section.Name(),
				pattern:    pattern,
				retentions: section.KeysHash()["RETENTIONS"],
				patternRegex: patternRegex,
			},
		)
	}
	return schemas, nil
}
