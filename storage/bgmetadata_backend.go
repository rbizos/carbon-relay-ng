package storage

type BgMetadataStorageConnector interface {
	UpdateMetricMetadata(metric *Metric) error
	InsertDirectory(dir *MetricDirectory) error
	SelectDirectory(dir string) (string, error)
}

type BgMetadataNoOpStorageConnector struct {
}

func (cc *BgMetadataNoOpStorageConnector) UpdateMetricMetadata(metric *Metric) error {
	return nil
}

func (cc *BgMetadataNoOpStorageConnector) InsertDirectory(dir *MetricDirectory) error {
	return nil
}

// SelectDirectory returns the parent directory or an error if it is not created
// empty string
func (cc *BgMetadataNoOpStorageConnector) SelectDirectory(dir string) (string, error) {
	return "", nil
}
