package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

	"github.com/omegaup/logslurp"
	"github.com/pkg/errors"
)

type clientConfig struct {
	URL string `json:"url"`
}

type logslurpConfig struct {
	Client           clientConfig             `json:"client"`
	OffsetFilePath   string                   `json:"offset_file"`
	StreamsDirectory string                   `json:"streams_directory,omitempty"`
	Labels           map[string]string        `json:"labels"`
	Streams          []*logslurp.StreamConfig `json:"streams,omitempty"`
}

func readLogslurpConfig(configPath string) (*logslurpConfig, error) {
	config := logslurpConfig{
		Client: clientConfig{
			URL: "https://loki.omegaup.com/loki/api/v1/push",
		},
		OffsetFilePath: "/var/lib/omegaup/logslurp_offsets.json",
	}
	if err := readJson(configPath, &config); err != nil {
		return nil, err
	}
	if config.StreamsDirectory != "" {
		config.StreamsDirectory = path.Join(path.Dir(configPath), config.StreamsDirectory)
		directoryEntries, err := ioutil.ReadDir(config.StreamsDirectory)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open streams directory %q", config.StreamsDirectory)
		}
		for _, directoryEntry := range directoryEntries {
			if directoryEntry.IsDir() {
				continue
			}
			var streamConfig logslurp.StreamConfig
			if err := readJson(path.Join(config.StreamsDirectory, directoryEntry.Name()), &streamConfig); err != nil {
				return nil, err
			}
			config.Streams = append(config.Streams, &streamConfig)
		}
	}
	// Merge any labels that were declared in the top-level config into all of the streams.
	for _, stream := range config.Streams {
		for k, v := range config.Labels {
			if _, ok := stream.Labels[k]; ok {
				// Do not override any labels that were set by the individual streams.
				continue
			}
			stream.Labels[k] = v
		}
	}

	return &config, nil
}

type offsetMapping struct {
	Offsets            map[string]int64              `json:"offsets"`
	OrphanedLogEntries []*logslurp.PushRequestStream `json:"orphaned_log_entries,omitempty"`
}

func readOffsetMapping(offsetMappingPath string) (offsetMapping, error) {
	offsets := offsetMapping{
		Offsets: make(map[string]int64),
	}
	err := readJson(offsetMappingPath, &offsets)
	return offsets, err
}

func (o *offsetMapping) write(offsetMappingPath string) error {
	f, err := os.Create(offsetMappingPath)
	if err != nil {
		return errors.Wrapf(err, "failed to open %q", offsetMappingPath)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(o); err != nil {
		return errors.Wrapf(err, "failed to write %q", offsetMappingPath)
	}
	return nil
}

func readJson(jsonPath string, v interface{}) error {
	f, err := os.Open(jsonPath)
	if err != nil {
		if os.IsNotExist(err) {
			return err
		}
		return errors.Wrapf(err, "failed to open %q", jsonPath)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return errors.Wrapf(err, "failed to parse %q", jsonPath)
	}
	return nil
}
