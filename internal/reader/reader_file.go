package reader

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type File struct {
	f      Mmap
	footer shared.FileFooter
	cache  Cache
	id     uint64
}

type hashKey struct {
	Id  uint64
	Pos int
}

var id uint64

// return pointer because cursor references it it so it can't be
// put in a list or moved otherwise
func NewFile(filename string, cache Cache) (*File, error) {
	r := &File{
		cache: cache,
		id:    atomic.AddUint64(&id, 1),
	}

	f, err := NewMmap(filename)
	if err != nil {
		return nil, err
	}
	r.f = f
	buf := f.Bytes
	footerSize := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	r.footer.Unmarshal(buf[len(buf)-4-int(footerSize):])
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
	hashKey := hashKey{
		Id:  r.id,
		Pos: pos,
	}

	v, ok := r.cache.Get(hashKey)
	if ok {
		b := v.(*block.ReadBlock)
		if b.Count > 0 { // count == 0 if closed
			return b
		}
	}

	block := block.Read(r.f.Bytes[pos:])
	r.cache.Add(hashKey, block)
	return block
}

func (r *File) Cursor() *Cursor {
	c := &Cursor{r: r}
	//block := block.Read(r.f.Bytes[r.footer.LastIndexPosition:])
	block := r.readBlock(r.footer.LastIndexPosition)
	c.indexes = append(c.indexes, NewIndex(block))
	return c
}

func (r *File) Close() error {
	return r.f.Close()
}
