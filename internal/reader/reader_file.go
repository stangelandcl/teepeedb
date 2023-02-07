package reader

import (
	"encoding/binary"
	"fmt"

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type File struct {
	f      Mmap
	footer shared.FileFooter
}

// return pointer because cursor references it it so it can't be
// put in a list or moved otherwise
func NewFile(filename string) (*File, error) {
	r := &File{}

	f, err := NewMmap(filename)
	if err != nil {
		return nil, err
	}
	r.f = f
	buf := f.Bytes
	footerSize := int(binary.LittleEndian.Uint32(buf[len(buf)-4:]))
	start := len(buf) - 4 - footerSize
	r.footer.Unmarshal(buf[start : start+footerSize])
	if r.footer.BlockFormat != 1 {
		f.Close()
		return nil, fmt.Errorf("teepeedb: invalid block format: %v", r.footer.BlockFormat)
	}

	return r, nil
}

func (r *File) Footer() shared.FileFooter {
	return r.footer
}

func (r *File) readBlock(pos int) *block.ReadBlock {
	return block.Read(r.f.Bytes[pos:])
}

func (r *File) Cursor() *Cursor {
	c := &Cursor{r: r}
	if r.footer.LastIndexPosition < 0 {
		// empty file
		return c
	}
	block := r.readBlock(r.footer.LastIndexPosition)
	c.indexes = append(c.indexes, NewIndex(block))
	return c
}

func (r *File) Close() error {
	return r.f.Close()
}
