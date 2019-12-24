package logslurp

import (
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/inconshreveable/log15"
	"github.com/pkg/errors"
)

func openNonBlocking(name string) (f *os.File, filesize int64, inode uint64, err error) {
	fd, err := syscall.Open(name, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
	if err != nil {
		return nil, 0, 0, errors.Wrapf(err, "could not open %q", name)
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		return nil, 0, 0, errors.Wrapf(err, "could not set %q as nonblocking", name)
	}

	f = os.NewFile((uintptr)(fd), name)
	fileinfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, 0, errors.Wrapf(err, "could not get file size for %q", name)
	}

	stat, ok := fileinfo.Sys().(*syscall.Stat_t)
	if !ok {
		f.Close()
		return nil, 0, 0, errors.Wrapf(err, "could not get file inode for %q", name)
	}

	filesize = stat.Size
	inode = stat.Ino
	err = nil

	return
}

type chunk struct {
	p   []byte
	n   int
	err error

	reply chan *chunk
}

// Tail is a ReadCloser
type Tail struct {
	stopChan chan struct{}
	doneChan chan struct{}
	file     *os.File
	off      int64
	inode    uint64
	name     string
	watcher  *fsnotify.Watcher
	chunks   chan *chunk
	log      log15.Logger
}

var _ io.ReadCloser = (*Tail)(nil)

// NewTail opens a file at the specified offset and provides an io.Reader
// interface to the file, tail(1)-style: if the file is moved and recreated
// (maybe due to a log rotation), this transparently reopens the file and keeps
// the stream open.
//
// This means that the calls to Read() will never return EOF, unless Stop() is
// called.
func NewTail(name string, off int64, log log15.Logger) (*Tail, error) {
	if resolved, err := filepath.Abs(name); err != nil {
		return nil, err
	} else {
		name = resolved
	}
	parent := filepath.Dir(name)

	t := &Tail{
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		off:      off,
		name:     name,
		chunks:   make(chan *chunk),
		log:      log,
	}
	if watcher, err := fsnotify.NewWatcher(); err != nil {
		return nil, err
	} else {
		t.watcher = watcher
	}

	go t.run()

	if err := t.watcher.Add(parent); err != nil {
		t.Stop()
		t.Close()
		return nil, errors.Wrapf(err, "could not watch %q", parent)
	}
	if f, filesize, inode, err := openNonBlocking(name); err != nil {
		t.Stop()
		t.Close()
		return nil, errors.Wrapf(err, "could not open %q", name)
	} else {
		atomic.StoreUint64(&t.inode, inode)
		t.file = f
		if t.off > filesize {
			// If the provided offset is larger than the file size, that means that the
			// file was truncated. We will read from the beginning.
			atomic.StoreInt64(&t.off, 0)
		}
	}

	return t, nil
}

// Offset returns the current offset of the file.
func (t *Tail) Offset() int64 {
	return atomic.LoadInt64(&t.off)
}

// Inode returns the inode of the currently opened file.
func (t *Tail) Inode() uint64 {
	return atomic.LoadUint64(&t.inode)
}

func (t *Tail) Read(p []byte) (n int, err error) {
	for {
		reply := make(chan *chunk)
		t.chunks <- &chunk{
			p:     p,
			reply: reply,
		}
		c, ok := <-reply
		if !ok {
			return 0, io.EOF
		}
		if c.n == 0 && c.err == nil {
			// This combination is a signal that we have reached the end of the file,
			// but more data can be read later.
			continue
		}
		return c.n, c.err
	}
}

// Stop immediately stops reading from the file. This causes EOF to be returned
// from Read().
func (t *Tail) Stop() {
	close(t.stopChan)
}

// Close frees any resources from the Tail instance.
func (t *Tail) Close() (finalErr error) {
	if err := t.watcher.Close(); err != nil {
		finalErr = errors.Wrap(err, "could not cleanly close the watcher")
	}

	// Wait until run() returns.
	<-t.doneChan
	close(t.chunks)

	if t.file != nil {
		if err := t.file.Close(); err != nil {
			finalErr = errors.Wrapf(err, "could not cleanly close %q", t.name)
		}
	}
	return
}

// drainChunks tries to perform as many Read operations on the file as the file
// will allow before reaching EOF.
func (t *Tail) drainChunks() {
	for chunk := range t.chunks {
		chunk.n, chunk.err = t.file.ReadAt(chunk.p, atomic.LoadInt64(&t.off))
		atomic.AddInt64(&t.off, int64(chunk.n))
		if chunk.err == io.EOF {
			// If we finished reading the file, we pretend that it's still going on.
			chunk.err = nil
		}
		chunk.reply <- chunk
		close(chunk.reply)
		if chunk.n == 0 {
			// But if the file really has no more contents, we stop trying to read
			// from it for the time being.
			return
		}
	}
}

func (t *Tail) run() {
	reportedError := io.EOF
	defer func() {
		close(t.doneChan)

		// Once we have signalled that we are done, drain whatever chunks are still
		// in the queue and report EOF (or whatever read error occurred.
		for chunk := range t.chunks {
			chunk.n = 0
			chunk.err = reportedError
			chunk.reply <- chunk
			close(chunk.reply)
		}
	}()

	// Read as much as possible.
	t.drainChunks()

	for {
		select {
		case <-t.stopChan:
			// Stop() was called.
			return

		case event, ok := <-t.watcher.Events:
			if !ok {
				// The channel was closed cleanly.
				return
			}
			if event.Name != t.name || event.Op == fsnotify.Chmod {
				// We don't care about this kind of event.
				break
			}

			// Read as much as possible.
			t.drainChunks()

			if event.Op&fsnotify.Create == fsnotify.Create {
				// But if the file was re-created, close it and open it again.
				t.file.Close()
				if f, _, inode, err := openNonBlocking(t.name); err != nil {
					reportedError = err
					return
				} else {
					t.log.Info("file was re-created", "path", t.name)
					t.file = f
					atomic.StoreInt64(&t.off, 0)
					atomic.StoreUint64(&t.inode, inode)
				}
			}

		case err, ok := <-t.watcher.Errors:
			if !ok {
				// The channel was closed cleanly.
				return
			}
			reportedError = err
			return
		}
	}
}
