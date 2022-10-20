package reader

import (
	"encoding/binary"
	"fmt"

	"github.com/stangelandcl/teepeedb/shared"
)

type File struct {
	f                  Mmap
	blockReader        BlockReader
	firstIndexPosition int
	fixedSize          int
}

type BlockReader interface {
	ReadBlock(pos int) ([]byte, error)
}

func NewFile(filename string, cache Cache) (File, error) {
	r := File{}

	f, err := NewMmap(filename)
	if err != nil {
		return r, err
	}
	r.f = f

	buf := f.Bytes
	compType := shared.Compression(buf[len(buf)-17])
	switch compType {
	case shared.Raw:
		r.blockReader, err = NewRaw(buf)
	case shared.Lz4:
		r.blockReader, err = NewLz4(buf, cache)
	default:
		err = fmt.Errorf("teepeedb: invalid compresstype: %v", compType)
	}
	if err != nil {
		f.Close()
		return File{}, err
	}
	r.fixedSize = int(int64(binary.LittleEndian.Uint64(buf[len(buf)-16:])))
	r.firstIndexPosition = int(int64(binary.LittleEndian.Uint64(buf[len(buf)-8:])))
	return r, nil
}

func (r *File) Cursor() (*Cursor, error) {
	c := &Cursor{
		f:         r.blockReader,
		fixedSize: r.fixedSize,
	}
	block, err := r.blockReader.ReadBlock(r.firstIndexPosition)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, NewIndex(block))
	return c, nil
}

func (r *File) Close() error {
	return r.f.Close()
}
