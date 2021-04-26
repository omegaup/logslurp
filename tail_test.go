package logslurp

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	base "github.com/omegaup/go-base/v2"
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

	tail, err := NewTail(filename, 0, base.StderrLog(false))
	if err != nil {
		t.Errorf("Failed to open file: %v", err)
	}
	originalInode := tail.Inode()

	doneChan := make(chan struct{})
	var buf bytes.Buffer
	go func() {
		if _, err := io.Copy(&buf, tail); err != nil {
			t.Errorf("Failed to read file: %v", err)
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
	if err := tail.Close(); err != nil {
		t.Errorf("Failed to close file: %v", err)
	}
	finalInode := tail.Inode()

	// There is a small problem here: tail.Stop() immediately stops reading from
	// the inotify stream. There is no easy way to synchronize both events
	// without poking into the implementation details of Tail.
	//
	// It is completely valid to have the contents of the stream be "1\n",
	// "1\n2\n", and "1\n\2\n3\n". In the two versions, there is no guarantee
	// about what the value of the final inode should be, since those events
	// could feasibly appear in the stream after calling `tail.Stop()`.
	if buf.String() == "1\n" {
		t.Logf("Very short version detected, inode should still be the old one")
		if finalInode != originalInode {
			t.Errorf("Failed to detect the new inode, got %v, want %v", finalInode, originalInode)
		}
		return
	}
	if buf.String() == "1\n2\n" {
		t.Logf("Short version detected, no guarantee about the inode")
		return
	}

	if buf.String() != "1\n2\n3\n" {
		t.Errorf("Failed to read file, got %q, want %q", buf.String(), "1\n2\n3\n")
	}
	if finalInode == originalInode {
		t.Errorf("Failed to read new inode, got %v, want !%v", finalInode, originalInode)
	}
}
