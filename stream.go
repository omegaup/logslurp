package logslurp

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

// StreamConfig represents an entry for one stream.
type StreamConfig struct {
	Path            string            `json:"path"`
	Labels          map[string]string `json:"labels"`
	RegexpString    string            `json:"regexp"`
	TimestampLayout string            `json:"timestamp_layout"`
}

// A PushRequestStreamEntry is a timestamp/log line value pair.
type PushRequestStreamEntry struct {
	Timestamp time.Time
	Line      string
}

var _ fmt.Stringer = (*PushRequestStreamEntry)(nil)
var _ json.Marshaler = (*PushRequestStreamEntry)(nil)

func (e *PushRequestStreamEntry) String() string {
	return fmt.Sprintf(
		"{Timestamp:%s, Line: %q}",
		e.Timestamp.UTC().Format("\"2006-01-02T15:04:05.000000-07:00\""),
		e.Line,
	)
}

// MarshalJSON returns the JSON encoding of the PushRequestStreamEntry.
func (e *PushRequestStreamEntry) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(
		"[\"%d\", %q]",
		e.Timestamp.UnixNano(),
		e.Line,
	)), nil
}

// A PushRequestStream is a stream of timestamp/log line values in ascending
// timestamp order, plus stream labels in key/value pair format.
type PushRequestStream struct {
	Stream map[string]string         `json:"stream"`
	Values []*PushRequestStreamEntry `json:"values"`
}

var _ fmt.Stringer = (*PushRequestStreamEntry)(nil)

func (s *PushRequestStream) String() string {
	return fmt.Sprintf(
		"{Stream:%s, Values:%s}",
		s.Stream,
		s.Values,
	)
}

// PushRequest is a message that can be sent to /loki/api/v1/push.
//
// This is documented at
// https://github.com/grafana/loki/blob/master/docs/api.md#post-lokiapiv1push
type PushRequest struct {
	Streams []*PushRequestStream `json:"streams"`
}

// A LogStream is used to parse a log given a StreamConfig.
type LogStream struct {
	rd     io.RuneReader
	regexp *regexp.Regexp
	config *StreamConfig
	slop   string
}

// NewLogStream returns a new instance of LogStream.
func NewLogStream(rd io.RuneReader, config *StreamConfig) (*LogStream, error) {
	s := &LogStream{
		rd:     rd,
		config: config,
	}
	r, err := regexp.Compile(s.config.RegexpString)
	if err != nil {
		return nil, err
	}
	s.regexp = r
	return s, nil
}

// A teeRuneReader is analogous to io.TeeReader but uses the io.RuneReader
// interface instead of io.Reader.
type teeRuneReader struct {
	r io.RuneReader
	w strings.Builder
}

var _ io.RuneReader = (*teeRuneReader)(nil)

func (t *teeRuneReader) ReadRune() (ch rune, size int, err error) {
	ch, size, err = t.r.ReadRune()
	if size > 0 {
		if _, err := t.w.WriteRune(ch); err != nil {
			return ch, size, err
		}
	}
	return
}

// A multiRuneReader is analogous to io.MultiReader but uses the io.RuneReader
// interface instead of io.Reader.
type multiRuneReader struct {
	r []io.RuneReader
}

var _ io.RuneReader = (*multiRuneReader)(nil)

func (m *multiRuneReader) ReadRune() (ch rune, size int, err error) {
	for {
		if len(m.r) == 0 {
			return 0, 0, io.EOF
		}
		ch, size, err = m.r[0].ReadRune()
		if err != io.EOF {
			return
		}
		m.r = m.r[1:]
	}
}

// Read returns the next log entry wrapped in a PushRequestStream so that it
// can retain the labels.
func (s *LogStream) Read() (*PushRequestStream, error) {
	t := teeRuneReader{
		r: &multiRuneReader{[]io.RuneReader{strings.NewReader(s.slop), s.rd}},
	}
	groupPairs := s.regexp.FindReaderSubmatchIndex(&t)
	if groupPairs == nil {
		return nil, io.EOF
	}

	entry := &PushRequestStreamEntry{}

	labels := map[string]string{
		"filename": s.config.Path,
	}
	for key, value := range s.config.Labels {
		labels[key] = value
	}
	var finalLine []string
	for i, label := range s.regexp.SubexpNames() {
		if i == 0 {
			// The first entry is always empty.
			continue
		}
		group := t.w.String()[groupPairs[2*i]:groupPairs[2*i+1]]
		if label == "ts" {
			ts, err := time.Parse(s.config.TimestampLayout, group)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse timestamp %q", group)
			}
			entry.Timestamp = ts
		} else if label != "" {
			labels[label] = group
		} else {
			finalLine = append(finalLine, group)
		}
	}
	entry.Line = strings.Join(finalLine, " ")
	s.slop = t.w.String()[groupPairs[len(groupPairs)-1]:]

	if time.Time(entry.Timestamp).IsZero() {
		entry.Timestamp = time.Now()
	}

	return &PushRequestStream{
		Values: []*PushRequestStreamEntry{entry},
		Stream: labels,
	}, nil
}

// NewPushRequest returns a PushRequest given a list of PushRequestStream
// objects. This groups the streams by their labels so that the final payload
// is smaller.
func NewPushRequest(streams []*PushRequestStream) *PushRequest {
	mapping := map[string]*PushRequestStream{}

	for _, logstream := range streams {
		var serializedLabels []string
		for k, v := range logstream.Stream {
			serializedLabels = append(serializedLabels, fmt.Sprintf("%s=%q", k, v))
		}
		sort.Strings(serializedLabels)

		mappingKey := strings.Join(serializedLabels, ",")
		if mappedStream, ok := mapping[mappingKey]; ok {
			mappedStream.Values = append(mappedStream.Values, logstream.Values...)
		} else {
			mapping[mappingKey] = &PushRequestStream{
				Stream: logstream.Stream,
				Values: logstream.Values,
			}
		}
	}

	request := &PushRequest{}
	for _, stream := range mapping {
		sort.Slice(stream.Values, func(i, j int) bool {
			return stream.Values[i].Timestamp.Before(stream.Values[j].Timestamp)
		})
		request.Streams = append(request.Streams, stream)
	}

	return request
}
