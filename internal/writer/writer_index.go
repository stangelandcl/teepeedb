package writer

import (
	"encoding/binary"

	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/varint"
)

type Index struct {
	b   Block
	buf []byte
}

func NewIndex(blockSize int) Index {
	return Index{
		b: NewBlock(blockSize, false),
	}
}

func (i *Index) HasSpace(key []byte, val shared.IndexValue) bool {
	kl := len(key)
	vl := varint.Len((val.Position<<1)|int(val.Type)) + varint.Len(len(val.LastKey)) + len(val.LastKey)
	return i.b.HasSpace(kl, vl)
}

func (i *Index) Add(key []byte, val shared.IndexValue) {
	i.buf = i.buf[:0]
	i.buf = binary.AppendUvarint(i.buf, uint64((val.Position<<1)|int(val.Type)))
	i.buf = binary.AppendUvarint(i.buf, uint64(len(val.LastKey)))
	i.buf = append(i.buf, val.LastKey...)
	kv := shared.KV{}
	kv.Key = key
	kv.Value = i.buf
	i.b.Add(&kv)
}

func (i *Index) Write(w BlockWriter) (Stats, error) {
	stats, err := i.b.Write(w)
	if err != nil {
		return stats, err
	}
	o := 0
	_ = varint.Read(i.buf, &o)  // value position + type
	n := varint.Read(i.buf, &o) // length of key
	stats.LastKey = make([]byte, n)
	copy(stats.LastKey, i.buf[o:o+n])
	return stats, nil
}
