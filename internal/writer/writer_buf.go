package writer

import (
	"bufio"
	"os"
)

// buffered file with position
type Buffered struct {
	w        *bufio.Writer
	f        *os.File
	Position int
	closed   bool
}

func NewBuffered(filename string) (*Buffered, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &Buffered{
		f: f,
		w: bufio.NewWriterSize(f, 8*1024*1024),
	}, nil
}
func (b *Buffered) Write(buf []byte) (int, error) {
	n, err := b.w.Write(buf)
	b.Position += n
	return n, err
}

func (b *Buffered) Commit() error {
	err1 := b.w.Flush()
	err2 := b.f.Sync()
	err3 := b.f.Close()
	b.closed = true
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

func (b *Buffered) Close() error {
	var err error
	if !b.closed {
		err = b.f.Close()
		b.closed = true
	}
	return err
}
