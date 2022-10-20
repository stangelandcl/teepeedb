package reader

import (
	"os"

	"github.com/stangelandcl/teepeedb/mmap"
)

type Mmap struct {
	Filename string
	f        *os.File
	Bytes    mmap.MMap
}

func NewMmap(filename string) (Mmap, error) {
	r := Mmap{
		Filename: filename,
	}
	f, err := os.Open(filename)
	if err != nil {
		return Mmap{}, err
	}
	buf, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()
		return Mmap{}, err
	}
	r.Bytes = buf
	r.f = f
	return r, nil
}

func (r *Mmap) Close() error {
	err1 := r.Bytes.Unmap()
	err2 := r.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
