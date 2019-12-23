package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/inconshreveable/log15"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/logslurp"
	"github.com/pkg/errors"
)

var (
	version        = flag.Bool("version", false, "Print the version and exit")
	configPath     = flag.String("config", "/etc/omegaup/logslurp/config.json", "configuration file")
	singleFile     = flag.String("single-file", "", "run the test the config of a single file, without following")
	singleFilePath = flag.String("single-file-path", "", "override for the single file path. useful for backfilling.")
	noOp           = flag.Bool("no-op", false, "do not upload the log entries on single file mode")

	// ProgramVersion is the version of the code from which the binary was built from.
	ProgramVersion string
)

const (
	maxBufferedMessages   = 10
	logEntryFlushInterval = 5 * time.Second
)

type Stream struct {
	config    *logslurp.StreamConfig
	tail      *logslurp.Tail
	logStream *logslurp.LogStream
	log       log15.Logger

	doneChan chan struct{}
	outChan  chan<- *logslurp.PushRequestStream
}

func (s *Stream) run() {
	s.log.Info("reading", "file", s.config.Path)
	for {
		logEntry, err := s.logStream.Read()
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
		s.outChan <- logEntry
	}

	s.log.Info("finished reading", "file", s.config.Path)
	s.tail.Close()
	close(s.doneChan)
}

func pushRequest(config *clientConfig, log log15.Logger, logEntries []*logslurp.PushRequestStream) error {
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
	config *clientConfig,
	log log15.Logger,
	outChan <-chan *logslurp.PushRequestStream,
	doneChan chan<- struct{},
) {
	defer close(doneChan)

	ticker := time.NewTicker(logEntryFlushInterval)
	defer ticker.Stop()

	var logEntries []*logslurp.PushRequestStream
	for {
		select {
		case <-ticker.C:
			// Once the flush interval has elapsed, send the pending log entries
			// regardless of how many there are.
			if err := pushRequest(config, log, logEntries); err != nil {
				log.Error("failed to push logs", "err", err)
			}
			logEntries = nil

		case logEntry, ok := <-outChan:
			if ok {
				logEntries = append(logEntries, logEntry)
			}
			if len(logEntries) > maxBufferedMessages || !ok {
				if err := pushRequest(config, log, logEntries); err != nil {
					log.Error("failed to push logs", "err", err)
				}
				logEntries = nil
				if !ok {
					// The channel has been closed. Push the final log entries and exit.
					return
				}
			}
		}
	}
}

func processSingleFile(logPath string, streamConfig *logslurp.StreamConfig) ([]*logslurp.PushRequestStream, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(logPath, ".gz") {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}

	l, err := logslurp.NewLogStream(bufio.NewReader(r), streamConfig)
	if err != nil {
		return nil, err
	}

	var logEntries []*logslurp.PushRequestStream
	for {
		logEntry, err := l.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to read a log entry")
		}
		logEntries = append(logEntries, logEntry)
	}
	return logEntries, nil
}

func main() {
	flag.Parse()
	if *version {
		fmt.Printf("omegaup-logslurp %s\n", ProgramVersion)
		return
	}

	log := base.StderrLog()

	config, err := readLogslurpConfig(*configPath)
	if err != nil {
		log.Error("failed to parse config", "err", err)
		os.Exit(1)
	}

	if *singleFile != "" {
		var streamConfig *logslurp.StreamConfig

		for _, streamConfigEntry := range config.Streams {
			if streamConfigEntry.Path == *singleFile {
				streamConfig = streamConfigEntry
				break
			}
		}
		if streamConfig == nil {
			log.Error("could not find config entry", "path", *singleFile)
			os.Exit(1)
		}

		if *singleFilePath == "" {
			*singleFilePath = *singleFile
		}
		logEntries, err := processSingleFile(*singleFilePath, streamConfig)
		if err != nil {
			log.Error("failed to test file", "path", *singleFilePath, "err", err)
			os.Exit(1)
		}
		if *noOp {
			for _, logEntry := range logEntries {
				log.Info("read entry", "entry", logEntry)
			}
		} else {
			if err := pushRequest(&config.Client, log, logEntries); err != nil {
				log.Error("failed to push logs", "err", err)
			}
		}

		return
	}

	if config.OffsetFilePath == "" {
		log.Error("missing 'offset_file' config entry")
		os.Exit(1)
	}
	offsets, err := readOffsetMapping(config.OffsetFilePath)
	if err != nil && !os.IsNotExist(err) {
		log.Error(
			"failed to parse offset mapping",
			"path", config.OffsetFilePath,
			"err", err,
		)
		os.Exit(1)
	}
	if err := offsets.write(config.OffsetFilePath); err != nil {
		log.Error(
			"failed to write offset mapping",
			"path", config.OffsetFilePath,
			"err", err,
		)
		os.Exit(1)
	}

	doneChan := make(chan struct{})
	outChan := make(chan *logslurp.PushRequestStream, 10)

	go readLoop(&config.Client, log, outChan, doneChan)

	var streams []*Stream
	for _, streamConfigEntry := range config.Streams {
		if _, err := os.Stat(streamConfigEntry.Path); os.IsNotExist(err) {
			log.Error(
				"failed to open file",
				"file", streamConfigEntry.Path,
				"err", err,
			)
			os.Exit(1)
		}

		s := &Stream{
			config:   streamConfigEntry,
			doneChan: make(chan struct{}),
			outChan:  outChan,
			log:      log,
		}

		off, _ := offsets[s.config.Path]
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
		offsets[s.config.Path] = s.tail.Offset()
	}
	close(outChan)
	<-doneChan

	if err := offsets.write(config.OffsetFilePath); err != nil {
		log.Error(
			"failed to write offset mapping",
			"path", config.OffsetFilePath,
			"err", err,
		)
		os.Exit(1)
	}

	log.Info("Stopped gracefully")
}
