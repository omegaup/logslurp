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
	noOp           = flag.Bool("no-op", false, "do not upload the log entries or write the offset file")

	// ProgramVersion is the version of the code from which the binary was built from.
	ProgramVersion string
)

const (
	maxBufferedMessages   = 10
	logEntryFlushInterval = 5 * time.Second
)

// Stream represents the streamed version of a log file.
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

func pushRequest(
	config *clientConfig,
	noOp bool,
	log log15.Logger,
	logEntries []*logslurp.PushRequestStream,
) ([]*logslurp.PushRequestStream, error) {
	if noOp {
		log.Info(
			"pushing log entries",
			"entries", logEntries,
		)
		return nil, nil
	}

	for len(logEntries) > 0 {
		var chunk, nextLogEntries []*logslurp.PushRequestStream
		if len(logEntries) > maxBufferedMessages {
			chunk = logEntries[:maxBufferedMessages]
			nextLogEntries = logEntries[maxBufferedMessages:]
		} else {
			chunk = logEntries
		}

		buf, err := json.Marshal(logslurp.NewPushRequest(chunk))
		if err != nil {
			return logEntries, errors.Wrap(err, "failed to marshal json")
		}

		res, err := http.Post(config.URL, "application/json", bytes.NewReader(buf))
		if err != nil {
			return logEntries, errors.Wrapf(err, "failed to push to %s", config.URL)
		}
		response, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode > 299 {
			return logEntries, errors.Errorf("failed to push to %s (%d): %q", config.URL, res.StatusCode, string(response))
		}

		logEntries = nextLogEntries
	}

	return logEntries, nil
}

func readLoop(
	config *clientConfig,
	noOp bool,
	log log15.Logger,
	outChan <-chan *logslurp.PushRequestStream,
	orphanedLogEntriesChan chan<- []*logslurp.PushRequestStream,
) {
	var logEntries []*logslurp.PushRequestStream
	defer func() {
		orphanedLogEntriesChan <- logEntries
		close(orphanedLogEntriesChan)
	}()

	ticker := time.NewTicker(logEntryFlushInterval)
	defer ticker.Stop()

	var err error

	for {
		select {
		case <-ticker.C:
			// Once the flush interval has elapsed, send the pending log entries
			// regardless of how many there are.
			logEntries, err = pushRequest(config, noOp, log, logEntries)
			if err != nil {
				log.Error("failed to push logs", "err", err, "queue length", len(logEntries))
			}

		case logEntry, ok := <-outChan:
			if ok {
				logEntries = append(logEntries, logEntry)
			}
			if len(logEntries) > maxBufferedMessages || !ok {
				logEntries, err = pushRequest(config, noOp, log, logEntries)
				if err != nil {
					log.Error("failed to push logs", "err", err, "queue length", len(logEntries))
				}
				if !ok {
					// The channel has been closed. Exit.
					return
				}
			}
		}
	}
}

func processSingleFile(
	config *clientConfig,
	noOp bool,
	log log15.Logger,
	logPath string,
	streamConfig *logslurp.StreamConfig,
) error {
	f, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(logPath, ".gz") {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gz.Close()
		r = gz
	}

	l, err := logslurp.NewLogStream(bufio.NewReader(r), streamConfig)
	if err != nil {
		return err
	}

	var logEntries []*logslurp.PushRequestStream
	for {
		logEntry, err := l.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "failed to read a log entry")
		}
		logEntries = append(logEntries, logEntry)
		if len(logEntries) > maxBufferedMessages {
			logEntries, err = pushRequest(config, noOp, log, logEntries)
			if err != nil {
				log.Error("failed to push logs", "err", err, "queue length", len(logEntries))
			}
		}
	}
	if len(logEntries) > 0 {
		logEntries, err = pushRequest(config, noOp, log, logEntries)
		if err != nil {
			return errors.Wrap(err, "failed to push logs")
		}
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
		if err := processSingleFile(&config.Client, *noOp, log, *singleFilePath, streamConfig); err != nil {
			log.Error("failed to test file", "path", *singleFilePath, "err", err)
			os.Exit(1)
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
	if !*noOp {
		if err := offsets.write(config.OffsetFilePath); err != nil {
			log.Error(
				"failed to write offset mapping",
				"path", config.OffsetFilePath,
				"err", err,
			)
			os.Exit(1)
		}
	}

	orphanedLogEntriesChan := make(chan []*logslurp.PushRequestStream)
	outChan := make(chan *logslurp.PushRequestStream, 10)

	go readLoop(&config.Client, *noOp, log, outChan, orphanedLogEntriesChan)

	if len(offsets.OrphanedLogEntries) > 0 {
		for _, logEntry := range offsets.OrphanedLogEntries {
			outChan <- logEntry
		}
		offsets.OrphanedLogEntries = nil
	}

	var streams []*Stream
	for _, streamConfigEntry := range config.Streams {
		fileinfo, err := os.Stat(streamConfigEntry.Path)
		if err != nil {
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

		var offset int64
		if off, ok := offsets.Offsets[s.config.Path]; ok {
			if stat, ok := fileinfo.Sys().(*syscall.Stat_t); ok {
				if off.Inode != stat.Ino {
					log.Warn(
						"file inode has change between restarts",
						"file", streamConfigEntry.Path,
						"original inode", off.Inode,
						"new inode", stat.Ino,
					)
				} else if off.Offset > stat.Size {
					log.Warn(
						"file was truncated between restarts",
						"file", streamConfigEntry.Path,
						"expected offset", off.Offset,
						"file size", stat.Size,
					)
				} else {
					offset = off.Offset
				}
			} else {
				log.Warn(
					"stat could not be converted to UNIX stat",
					"file", streamConfigEntry.Path,
				)
			}
		}
		if t, err := logslurp.NewTail(s.config.Path, offset, log); err != nil {
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
		offsets.Offsets[s.config.Path] = fileOffset{
			Offset: s.tail.Offset(),
			Inode:  s.tail.Inode(),
		}
	}
	close(outChan)
	for orphanedLogEntries := range orphanedLogEntriesChan {
		offsets.OrphanedLogEntries = append(offsets.OrphanedLogEntries, orphanedLogEntries...)
	}

	if *noOp {
		log.Info(
			"writing offset mapping",
			"contents", offsets,
		)
	} else {
		if err := offsets.write(config.OffsetFilePath); err != nil {
			log.Error(
				"failed to write offset mapping",
				"path", config.OffsetFilePath,
				"err", err,
			)
			os.Exit(1)
		}
	}

	log.Info("Stopped gracefully")
}
