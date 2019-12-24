package logslurp

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	base "github.com/omegaup/go-base"
)

func TestTail(t *testing.T) {
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		t.Errorf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(dirname)

	filename := path.Join(dirname, "foo")
	rotatedFilename := path.Join(dirname, "bar")
	f, err := os.Create(filename)
	if err != nil {
		t.Errorf("Failed to create file: %v", err)
	}

	tail, err := NewTail(filename, 0, base.StderrLog())
	if err != nil {
		t.Errorf("Failed to open file: %v", err)
	}
	originalInode := tail.Inode()

	doneChan := make(chan struct{})
	go func() {
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, tail); err != nil {
			t.Errorf("Failed to read file: %v", err)
		}
		if err := tail.Close(); err != nil {
			t.Errorf("Failed to close file: %v", err)
		}
		if buf.String() != "1\n2\n3\n" {
			t.Errorf("Failed to read file, got %q, want %q", buf.String(), "1\n2\n3\n")
		}
		close(doneChan)
	}()

	f.WriteString("1\n")
	f.Sync()
	if err := os.Rename(filename, rotatedFilename); err != nil {
		panic(err)
	}
	if err := os.Remove(rotatedFilename); err != nil {
		panic(err)
	}
	f.WriteString("2\n")
	f.Close()

	g, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	g.WriteString("3\n")
	g.Sync()
	g.Close()

	tail.Stop()

	<-doneChan
	finalInode := tail.Inode()

	if finalInode == originalInode {
		t.Errorf("Failed to read new inode, got %v, want !%v", finalInode, originalInode)
	}
}
