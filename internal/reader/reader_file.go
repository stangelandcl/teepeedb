package reader

import (
	"encoding/binary"
	"fmt"

	"github.com/stangelandcl/teepeedb/internal/shared"
)

type File struct {
	f           Mmap
	blockReader BlockReader
	footer      shared.FileFooter
}

type BlockReader interface {
	ReadBlock(pos int) []byte
}

func NewFile(filename string, cache Cache) (File, error) {
	r := File{}

	f, err := NewMmap(filename)
	if err != nil {
		return r, err
	}
	r.f = f
	buf := f.Bytes
	footerSize := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	r.footer.Unmarshal(buf[len(buf)-4-int(footerSize):])
	switch r.footer.BlockFormat {
	case shared.Raw:
		r.blockReader, err = NewRaw(buf)
	case shared.Lz4:
		r.blockReader, err = NewLz4(buf, cache)
	default:
		err = fmt.Errorf("teepeedb: invalid compresstype: %v", r.footer.BlockFormat)
	}
	if err != nil {
		f.Close()
		return File{}, err
	}
	return r, nil
}

func (r *File) Footer() shared.FileFooter {
	return r.footer
}

func (r *File) Cursor() *Cursor {
	c := &Cursor{
		f:         r.blockReader,
		fixedSize: r.footer.ValueSize,
	}
	block := r.blockReader.ReadBlock(r.footer.LastIndexPosition)
	c.indexes = append(c.indexes, NewIndex(block))
	return c
}

func (r *File) Close() error {
	return r.f.Close()
}
