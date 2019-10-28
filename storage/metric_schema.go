package storage

import "gopkg.in/ini.v1"

type StorageSchema struct {
	ID         string
	pattern    string
	retentions string
}

func NewStorageSchemas(storageSchemasConf string) ([]StorageSchema, error) {
	schemas := []StorageSchema{}
	cfg, err := ini.Load(storageSchemasConf)
	if err != nil {
		return schemas, nil
	}
	for _, section := range cfg.Sections()[1:] { // first element is empty default value
		schemas = append(
			schemas,
			StorageSchema{
				ID:         section.Name(),
				pattern:    section.KeysHash()["PATTERN"],
				retentions: section.KeysHash()["RETENTIONS"],
			},
		)
	}
	return schemas, nil
}
