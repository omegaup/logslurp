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

// ConfigEntry represents an entry for one stream.
type ConfigEntry struct {
	Path            string            `json:"path"`
	Labels          map[string]string `json:"labels"`
	RegexpString    string            `json:"regexp"`
	TimestampLayout string            `json:"timestamp_layout"`
}

// Time is identital to time.Time, except it implements the json.Marshaler
// interface with a format that's acceptable by Loki.
type Time time.Time

var _ json.Marshaler = (*Time)(nil)

func (t *Time) MarshalJSON() ([]byte, error) {
	return []byte(t.Format("\"2006-01-02T15:04:05.000000-07:00\"")), nil
}

func (t Time) Format(layout string) string {
	return (time.Time)(t).Format(layout)
}

func (t Time) Before(o Time) bool {
	return (time.Time)(t).Before((time.Time)(o))
}

type PushRequestStreamEntry struct {
	Timestamp Time   `json:"ts"`
	Line      string `json:"line"`
}

var _ fmt.Stringer = (*PushRequestStreamEntry)(nil)

func (e *PushRequestStreamEntry) String() string {
	return fmt.Sprintf(
		"{Timestamp:%s, Line: %q}",
		e.Timestamp.Format("\"2006-01-02T15:04:05.000000-07:00\""),
		e.Line,
	)
}

type PushRequestStream struct {
	Labels  string                    `json:"labels"`
	Entries []*PushRequestStreamEntry `json:"entries"`
}

type PushRequest struct {
	Streams []*PushRequestStream `json:"streams"`
}

// A LogStream is used to parse a log given a ConfigEntry.
type LogStream struct {
	rd     io.RuneReader
	regexp *regexp.Regexp
	config *ConfigEntry
	slop   string
}

// NewLogStream returns a new instance of LogStream.
func NewLogStream(rd io.RuneReader, config *ConfigEntry) (*LogStream, error) {
	s := &LogStream{
		rd:     rd,
		config: config,
	}
	if r, err := regexp.Compile(s.config.RegexpString); err != nil {
		return nil, err
	} else {
		s.regexp = r
	}
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
			if ts, err := time.Parse(s.config.TimestampLayout, group); err != nil {
				return nil, errors.Wrapf(err, "failed to parse timestamp \"%s\"", group)
			} else {
				entry.Timestamp = Time(ts)
			}
		} else if label != "" {
			labels[label] = group
		} else {
			finalLine = append(finalLine, group)
		}
	}
	entry.Line = strings.Join(finalLine, " ")
	s.slop = t.w.String()[groupPairs[len(groupPairs)-1]:]

	if time.Time(entry.Timestamp).IsZero() {
		entry.Timestamp = Time(time.Now())
	}

	result := &PushRequestStream{
		Entries: []*PushRequestStreamEntry{entry},
	}

	var serializedLabels []string
	for k, v := range labels {
		serializedLabels = append(serializedLabels, fmt.Sprintf("%s=%q", k, v))
	}
	sort.Strings(serializedLabels)

	result.Labels = fmt.Sprintf("{%s}", strings.Join(serializedLabels, ","))
	return result, nil
}

// NewPushRequest returns a PushRequest given a list of PushRequestStream
// objects. This groups the streams by their labels so that the final payload
// is smaller.
func NewPushRequest(streams []*PushRequestStream) *PushRequest {
	mapping := map[string][]*PushRequestStreamEntry{}

	for _, Logstream := range streams {
		if _, ok := mapping[Logstream.Labels]; ok {
			mapping[Logstream.Labels] = append(mapping[Logstream.Labels], Logstream.Entries...)
		} else {
			mapping[Logstream.Labels] = append([]*PushRequestStreamEntry{}, Logstream.Entries...)
		}
	}

	request := &PushRequest{}
	for labels, entries := range mapping {
		stream := &PushRequestStream{
			Labels:  labels,
			Entries: entries,
		}
		sort.Slice(stream.Entries, func(i, j int) bool {
			return stream.Entries[i].Timestamp.Before(stream.Entries[j].Timestamp)
		})
		request.Streams = append(request.Streams, stream)
	}

	return request
}
