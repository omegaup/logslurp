package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/inconshreveable/log15"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/logslurp"
	"github.com/pkg/errors"
)

var (
	version    = flag.Bool("version", false, "Print the version and exit")
	configPath = flag.String("config", "", "configuration file")
	testFile   = flag.String("test-file", "", "test the config for a single file")

	// ProgramVersion is the version of the code from which the binary was built from.
	ProgramVersion string
)

type ClientConfig struct {
	URL string `json:"url"`
}

type Config struct {
	Client         ClientConfig            `json:"client"`
	OffsetFilename string                  `json:"offset_file"`
	Entries        []*logslurp.ConfigEntry `json:"entries"`
}

type Stream struct {
	config    *logslurp.ConfigEntry
	tail      *logslurp.Tail
	logStream *logslurp.LogStream
	log       log15.Logger

	doneChan chan struct{}
	outChan  chan<- *logslurp.PushRequestStream
}

func (s *Stream) run() {
	s.log.Info("reading", "file", s.config.Path)
	for {
		l, err := s.logStream.Read()
		if err != nil {
			if err == io.EOF {
				s.log.Error(
					"eof while reading file",
					"file", s.config.Path,
				)
			} else {
				s.log.Error(
					"failed to read log file",
					"file", s.config.Path,
					"err", err,
				)
			}
			break
		}
		s.outChan <- l
	}

	s.log.Info("finished reading", "file", s.config.Path)
	s.tail.Close()
	close(s.doneChan)
}

func readJson(filename string, missingOk bool, v interface{}) error {
	f, err := os.Open(filename)
	if err != nil {
		if missingOk && os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to open \"%s\"", filename)
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(v); err != nil {
		return errors.Wrapf(err, "failed to parse \"%s\"", filename)
	}
	return nil
}

func writeJson(filename string, v interface{}) error {
	f, err := os.Create(filename)
	if err != nil {
		return errors.Wrapf(err, "failed to open \"%s\"", filename)
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(v); err != nil {
		return errors.Wrapf(err, "failed to write \"%s\"", filename)
	}
	return nil
}

func pushRequest(config *ClientConfig, log log15.Logger, logEntries []*logslurp.PushRequestStream) error {
	if len(logEntries) == 0 {
		return nil
	}

	buf, err := json.Marshal(logslurp.NewPushRequest(logEntries))
	if err != nil {
		return errors.Wrap(err, "failed to marshal json")
	}

	res, err := http.Post(config.URL, "application/json", bytes.NewReader(buf))
	if err != nil {
		return errors.Wrapf(err, "failed to push to %s", config.URL)
	}
	response, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if len(response) != 0 {
		log.Error("sent a chunk", "response", string(response))
	}

	return nil
}

func readLoop(
	config *ClientConfig,
	log log15.Logger,
	outChan <-chan *logslurp.PushRequestStream,
	doneChan chan<- struct{},
) {
	defer close(doneChan)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var logEntries []*logslurp.PushRequestStream
	for {
		select {
		case <-ticker.C:
			if err := pushRequest(config, log, logEntries); err != nil {
				log.Error("failed to push logs", "err", err)
			}
			logEntries = nil

		case logEntry, ok := <-outChan:
			if !ok {
				if err := pushRequest(config, log, logEntries); err != nil {
					log.Error("failed to push logs", "err", err)
				}
				return
			}
			logEntries = append(logEntries, logEntry)
			if len(logEntries) > 10 {
				if err := pushRequest(config, log, logEntries); err != nil {
					log.Error("failed to push logs", "err", err)
				}
				logEntries = nil
			}
		}
	}
}

func processTestFile(path string, config *Config, log log15.Logger) error {
	var configEntry *logslurp.ConfigEntry

	for _, e := range config.Entries {
		if e.Path == path {
			configEntry = e
			break
		}
	}
	if configEntry == nil {
		return errors.Errorf("could not find config entry for \"%s\"", path)
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	l, err := logslurp.NewLogStream(bufio.NewReader(f), configEntry)
	if err != nil {
		return err
	}

	for {
		entry, err := l.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		log.Info("read entry", "entry", entry)
	}

	return nil
}

func main() {
	flag.Parse()
	if *version {
		fmt.Printf("omegaup-logslurp %s\n", ProgramVersion)
		return
	}

	log := base.StderrLog()

	if *configPath == "" {
		log.Error("missing -config argument")
		os.Exit(1)
	}

	var config Config
	if err := readJson(*configPath, false, &config); err != nil {
		log.Error("failed to parse config", "err", err)
		os.Exit(1)
	}

	if *testFile != "" {
		if err := processTestFile(*testFile, &config, log); err != nil {
			log.Error("failed to test file", "err", err)
		}
		return
	}

	if config.OffsetFilename == "" {
		log.Error("missing 'offset_file' config entry")
		os.Exit(1)
	}

	offsetMapping := make(map[string]int64)
	if err := readJson(config.OffsetFilename, true, &offsetMapping); err != nil {
		log.Error(
			"failed to parse offset mapping",
			"path", config.OffsetFilename,
			"err", err,
		)
		os.Exit(1)
	}
	if err := writeJson(config.OffsetFilename, offsetMapping); err != nil {
		log.Error(
			"failed to write offset mapping",
			"path", config.OffsetFilename,
			"err", err,
		)
		os.Exit(1)
	}

	doneChan := make(chan struct{})
	outChan := make(chan *logslurp.PushRequestStream, 10)

	go readLoop(&config.Client, log, outChan, doneChan)

	var streams []*Stream
	for _, entry := range config.Entries {
		s := &Stream{
			config:   entry,
			doneChan: make(chan struct{}),
			outChan:  outChan,
			log:      log,
		}

		off, _ := offsetMapping[s.config.Path]
		if t, err := logslurp.NewTail(s.config.Path, off, log); err != nil {
			log.Error(
				"failed to open file",
				"file", s.config.Path,
				"err", err,
			)
			os.Exit(1)
		} else {
			s.tail = t
		}

		if l, err := logslurp.NewLogStream(bufio.NewReader(s.tail), s.config); err != nil {
			log.Error(
				"failed to open log stream",
				"file", s.config.Path,
				"err", err,
			)
			os.Exit(1)
		} else {
			s.logStream = l
		}

		go s.run()

		streams = append(streams, s)
	}

	log.Info(
		"omegaup-logslurp ready",
		"version", ProgramVersion,
	)
	daemon.SdNotify(false, "READY=1")

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	<-stopChan

	daemon.SdNotify(false, "STOPPING=1")
	log.Info("Stopping...")

	for _, s := range streams {
		s.tail.Stop()
		<-s.doneChan
		offsetMapping[s.config.Path] = s.tail.Offset()
	}
	close(outChan)
	<-doneChan

	if err := writeJson(config.OffsetFilename, offsetMapping); err != nil {
		log.Error(
			"failed to write offset mapping",
			"path", config.OffsetFilename,
			"err", err,
		)
		os.Exit(1)
	}

	log.Info("Stopped gracefully")
}
