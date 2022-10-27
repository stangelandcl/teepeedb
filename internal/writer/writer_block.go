package writer

import (
	"encoding/binary"
	"io"

	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/varint"
)

type Block struct {
	body             []byte
	offsets          []uint16
	blockSize        int
	upserts, deletes int
	fixedSize        bool
	total            []byte
}

type Stats struct {
	FirstKey, LastKey []byte
	Upserts, Deletes  int
}

type BlockWriter interface {
	// block reader experts [uvarint len(offsets), offsets, body].
	// also writes a block header for total length and possibly compresses one
	// or more parts
	WriteBlock(offsets []uint16, body []byte) error
}

func NewBlock(blockSize int, fixedSizeValue bool) Block {
	w := Block{
		blockSize: blockSize,
		fixedSize: fixedSizeValue,
	}
	return w
}

func (b *Block) HasSpace(keylen, vallen int) bool {
	sz := varint.Len(keylen<<1) + keylen
	if !b.fixedSize {
		sz += varint.Len(vallen)
	}

	total := sz + len(b.body) + len(b.offsets) +
		varint.Len(len(b.body))*2 +
		varint.Len(len(b.offsets)/2)
	return len(b.offsets) == 0 || total <= b.blockSize
}

// use HasSpace() to check first
func (b *Block) Add(kv *shared.KV) {
	b.offsets = append(b.offsets, uint16(len(b.body)))
	delete := 0
	if kv.Delete {
		delete = 1
		b.deletes++
	} else {
		b.upserts++
	}
	b.body = binary.AppendUvarint(b.body, uint64((len(kv.Key)<<1)|delete))
	b.body = append(b.body, kv.Key...)
	if !b.fixedSize {
		b.body = binary.AppendUvarint(b.body, uint64(len(kv.Value)))
	}
	b.body = append(b.body, kv.Value...)
}

// returns io.EOF on no data to flush
func (b *Block) Write(w BlockWriter) (stats Stats, err error) {
	if len(b.body) == 0 {
		err = io.EOF
		return
	}

	err = w.WriteBlock(b.offsets, b.body)
	if err != nil {
		return
	}

	stats.FirstKey = b.readKey(0)
	stats.LastKey = b.readKey(len(b.offsets)/2 - 1)
	stats.Upserts = b.upserts
	stats.Deletes = b.deletes

	b.offsets = b.offsets[:0]
	b.body = b.body[:0]
	return stats, nil
}

func (b *Block) readKey(idx int) []byte {
	o := int(b.offsets[idx])
	ks := varint.Read(b.body, &o)
	ks >>= 1 // don't care if it is delete
	key := b.body[o : o+ks]
	tmp := make([]byte, len(key))
	copy(tmp, key)
	return tmp
}
