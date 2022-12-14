package merge

import (
	"fmt"
	"sync/atomic"

	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type Reader struct {
	files    []*reader.File
	refcount int64
}

type Stats struct {
	Footers []shared.FileFooter
}

// files in sorted order. newest first
func NewReader(files []string) (*Reader, error) {
	r := &Reader{refcount: 1}
	for _, f := range files {
		fr, err := reader.NewFile(f)
		if err != nil {
			for _, f := range r.files {
				f.Close()
			}
			return nil, fmt.Errorf("teepeedb: merge reader error opening %v: %v", f, err)
		}
		r.files = append(r.files, fr)
	}
	return r, nil
}

func (r *Reader) Stats() Stats {
	s := Stats{}
	for _, file := range r.files {
		s.Footers = append(s.Footers, file.Footer())
	}
	return s
}

func (r *Reader) Cursor() *Cursor {
	c := &Cursor{
		reader: r,
	}
	if atomic.AddInt64(&r.refcount, 1) <= 1 {
		atomic.AddInt64(&r.refcount, -1)
		return c // already closed
	}
	for _, f := range r.files {
		cur := f.Cursor()
		c.cursors = append(c.cursors, cur)
	}
	return c
}

// decrement refcount and close reader if this call came
// from the last user
func (r *Reader) Close() {
	// refcount = 0 requires all cursors closed plus
	// first call to Close() outside a cursor.Close()
	if atomic.AddInt64(&r.refcount, -1) != 0 {
		return
	}
	for _, f := range r.files {
		f.Close()
	}
	r.files = nil
}
