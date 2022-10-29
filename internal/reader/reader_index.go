package reader

import (
	"bytes"

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/varint"
)

type Index struct {
	b Block
}
type IndexKV struct {
	Key []byte
	shared.IndexValue
}

// readers are lightweight and can be recreated for each block read
func NewIndex(rb *block.ReadBlock) Index {
	return Index{
		b: NewBlock(rb, -1),
	}
}

func convert(key, val []byte) (ikv IndexKV) {
	ikv.Key = key
	pos := 0
	p := varint.Read(val, &pos)
	ikv.Position = p >> 1
	ikv.Type = shared.BlockType(p & 1)
	ikv.LastKey = val[pos:]
	return ikv
}

func (r *Index) Get() IndexKV {
	k, _ := r.b.Key(r.b.idx)
	v := r.b.Value(r.b.idx)
	return convert(k, v)
}

func (r *Index) LessOrEqual(find []byte) bool {
	c := r.b.Find(find, true)
	switch c {
	case Found:
		return true
	case NotFound:
		return false
	}

	ikv := r.Get()
	return bytes.Compare(find, ikv.LastKey) <= 0
}

func (r *Index) Move(dir Move) bool {
	switch dir {
	case First, Last, Next, Previous:
		return r.b.Move(dir)
	}
	return false
}

func (b *Index) InRange(key []byte) bool {
	k, _ := b.b.rb.Key(0)
	if bytes.Compare(key, k) < 0 {
		return false
	}
	v := b.b.rb.Value(b.b.Len() - 1)
	ikv := convert(nil, v)
	return bytes.Compare(key, ikv.LastKey) <= 0
}
