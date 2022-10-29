package writer

import (
	"encoding/binary"
	"io"

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/varint"
)

type Index struct {
	b   block.WriteBlock
	buf []byte
}

func (i *Index) HasSpace(key []byte, val shared.IndexValue, blockSize int) bool {
	kl := len(key)
	vl := varint.Len((val.Position<<1)|int(val.Type)) + varint.Len(len(val.LastKey)) + len(val.LastKey)
	return i.b.HasSpace(kl, vl, blockSize)
}

func (i *Index) Add(key []byte, val shared.IndexValue) {
	i.buf = i.buf[:0]
	i.buf = binary.AppendUvarint(i.buf, uint64((val.Position<<1)|int(val.Type)))
	i.buf = append(i.buf, val.LastKey...)
	kv := shared.KV{}
	kv.Key = key
	kv.Value = i.buf
	i.b.Put(kv.Key, kv.Value, kv.Delete)
}

func (i *Index) Write(f io.Writer, w *block.Writer) (block.Stats, error) {
	stats, err := w.Write(f, &i.b)
	if err != nil {
		return stats, err
	}
	pos := 0
	_ = varint.Read(i.buf, &pos) // value position + type
	stats.LastKey = make([]byte, len(i.buf)-pos)
	copy(stats.LastKey, i.buf[pos:])
	return stats, nil
}
