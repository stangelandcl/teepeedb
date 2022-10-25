package merge

import (
	"log"
	"sync"

	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type Reader struct {
	files   []reader.File
	mutex   sync.Mutex
	cursors int
}

type Stats struct {
	Footers []shared.FileFooter
}

// files in sorted order. newest first
func NewReader(files []string, cache reader.Cache) (*Reader, error) {
	r := &Reader{}
	for _, f := range files {
		fr, err := reader.NewFile(f, cache)
		if err != nil {
			for _, f := range r.files {
				f.Close()
			}
			log.Printf("merge reader error opening %v: %v\n", f, err)
			return nil, err
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

func (r *Reader) Cursor() (*Cursor, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	c := &Cursor{
		reader: r,
	}
	for _, f := range r.files {
		cur, err := f.Cursor()
		if err != nil {
			return c, err
		}
		c.cursors = append(c.cursors, cur)
	}
	r.cursors++
	return c, nil
}

func (r *Reader) Close() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.cursors > 0 {
		return
	}

	for _, f := range r.files {
		f.Close()
	}
	r.files = nil
}
