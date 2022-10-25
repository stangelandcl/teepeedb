package reader

import (
	"bytes"

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
func NewIndex(buf []byte) Index {
	return Index{
		b: NewBlock(buf, -1),
	}
}

func convert(ikv *IndexKV, kv *shared.KV) {
	ikv.Key = kv.Key
	pos := 0
	p := varint.Read(kv.Value, &pos)
	ikv.Position = p >> 1
	ikv.Type = shared.BlockType(p & 1)
	sz := varint.Read(kv.Value, &pos)
	ikv.LastKey = kv.Value[pos : pos+sz]
}

func (r *Index) LessOrEqual(find *IndexKV) bool {
	kv := shared.KV{}
	kv.Key = find.Key
	if r.b.Find(&kv, true) > 0 {
		convert(find, &kv)
		return true
	}

	k := find.Key
	convert(find, &kv)
	return /*bytes.Compare(k, ikv.Key) >= 0 &&*/ bytes.Compare(k, find.LastKey) <= 0
}

func (r *Index) Move(dir Move, ikv *IndexKV) bool {
	switch dir {
	case First, Last, Next, Previous:
		kv := shared.KV{}
		more := r.b.Move(dir, &kv)
		if more {
			convert(ikv, &kv)
		}
		return more
	}
	return false
}

func (r *Index) Print() {
	r.b.Print()
}

func (b *Index) InRange(kv *shared.KV) bool {
	k, _, _ := b.b.At(0)
	if bytes.Compare(kv.Key, k) < 0 {
		return false
	}
	_, v, _ := b.b.At(b.b.Len() - 1)
	ikv := IndexKV{}
	kv2 := shared.KV{
		Value: v,
	}
	convert(&ikv, &kv2)

	return bytes.Compare(kv.Key, ikv.LastKey) <= 0
}
