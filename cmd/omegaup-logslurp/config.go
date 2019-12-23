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
	Streams          []*logslurp.StreamConfig `json:"streams,omitempty"`
}

func readLogslurpConfig(configPath string) (*logslurpConfig, error) {
	config := logslurpConfig{
		Client: clientConfig{
			URL: "https://loki.omegaup.com/api/prom/push",
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

	return &config, nil
}

type offsetMapping map[string]int64

func readOffsetMapping(offsetMappingPath string) (offsetMapping, error) {
	offsets := make(offsetMapping)
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
