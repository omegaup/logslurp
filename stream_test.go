package logslurp

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

const logFileContents = `
xxx
2019-05-13T15:13:51+00:00 [ERROR]: Message 1 
xxx
2019-05-13T15:13:52+00:00 [ERROR]: Message 2 
2019-05-13T15:13:53+00:00 [ERROR]: Message 3
in two lines 
2019-05-13T15:13:54+00:00 [ERROR]: Message 4
in three
lines 
`

func TestStream(t *testing.T) {
	config := StreamConfig{
		Path:            "log",
		RegexpString:    `(?ms)^(?P<ts>\d+-\d+-\d+T\d+:\d+:\d+[-+]\d+:\d+) \[(?P<lvl>[^\]]+)\]: (.*?) $`,
		TimestampLayout: "2006-01-02T15:04:05-07:00",
	}

	s, err := NewLogStream(strings.NewReader(logFileContents), &config)
	if err != nil {
		t.Errorf("Failed to create stream: %v", err)
	}
	var entries []*PushRequestStream
	var stringEntries []string
	for {
		entry, err := s.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("Failed to read stream: %v", err)
		}
		entries = append(entries, entry)
		if s, err := json.Marshal(entry); err != nil {
			t.Fatalf("Failed to marshal: %v", err)
		} else {
			stringEntries = append(stringEntries, string(s))
		}
	}

	expectedStringEntries := []string{
		`{"stream":{"filename":"log","lvl":"ERROR"},"values":[["1557760431000000000","Message 1"]]}`,
		`{"stream":{"filename":"log","lvl":"ERROR"},"values":[["1557760432000000000","Message 2"]]}`,
		`{"stream":{"filename":"log","lvl":"ERROR"},"values":[["1557760433000000000","Message 3\nin two lines"]]}`,
		`{"stream":{"filename":"log","lvl":"ERROR"},"values":[["1557760434000000000","Message 4\nin three\nlines"]]}`,
	}

	if !reflect.DeepEqual(stringEntries, expectedStringEntries) {
		t.Errorf("failed to read stream. got %v, expected %v", stringEntries, expectedStringEntries)
	}

	request := NewPushRequest(entries)
	if len(request.Streams) != 1 {
		t.Errorf("failed to coalesce stream. got %v, expected 1", request)
	}
	expectedRequest := `{"streams":[{"stream":{"filename":"log","lvl":"ERROR"},"values":[["1557760431000000000","Message 1"],["1557760432000000000","Message 2"],["1557760433000000000","Message 3\nin two lines"],["1557760434000000000","Message 4\nin three\nlines"]]}]}`
	if marshaledRequest, err := json.Marshal(request); err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	} else if string(marshaledRequest) != expectedRequest {
		t.Fatalf("unexpected request. got %q, expected %q", string(marshaledRequest), expectedRequest)
	}
}

func TestPushRequestStreamStringer(t *testing.T) {
	request := NewPushRequest([]*PushRequestStream{
		{
			Stream: map[string]string{
				"filename": "log",
				"lvl":      "ERROR",
			},
			Values: []*PushRequestStreamEntry{
				{
					Timestamp: time.Unix(0, 0),
					Line:      "Hello, world!",
				},
			},
		},
	})

	gotString := fmt.Sprintf("%s", request)
	expectedString := `&{[{Stream:map[filename:log lvl:ERROR], Values:[{Timestamp:"1970-01-01T00:00:00.000000+00:00", Line: "Hello, world!"}]}]}`
	if gotString != expectedString {
		t.Errorf("failed to stringify stream. got %v, expected %v", gotString, expectedString)
	}
}
