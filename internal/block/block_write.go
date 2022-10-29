package block

import (
	"log"
	"math"

	"github.com/stangelandcl/teepeedb/internal/varint"
)

type WriteBlock struct {
	KeyOffsets []uint16
	ValOffsets []uint16
	Keys       []byte
	Vals       []byte
}

type Stats struct {
	FirstKey, LastKey []byte
	Upserts, Deletes  int
}

// for returning first and last key in block from write
func (b *WriteBlock) KeyAt(i int) []byte {
	n := len(b.Keys)
	end := n
	if i+1 < len(b.KeyOffsets) {
		end = int(b.KeyOffsets[i+1]) >> 1
	}
	return b.Keys[b.KeyOffsets[i]>>1 : end]
}

func (b *WriteBlock) Put(key, val []byte, delete bool) {
	if len(b.Keys) > math.MaxInt16 || len(b.Vals) > math.MaxUint16 {
		log.Panicln("block size out of range. offset > 32767")
	}
	n := len(b.Keys) << 1
	if delete {
		n |= 1
	}
	b.KeyOffsets = append(b.KeyOffsets, uint16(n))
	b.Keys = append(b.Keys, key...)

	b.ValOffsets = append(b.ValOffsets, uint16(len(b.Vals)))
	b.Vals = append(b.Vals, val...)
}

func (b *WriteBlock) Size() int {
	n := len(b.KeyOffsets) + len(b.Keys)
	sz := varint.Len(n) * 2             // *2 to estimate compressed length
	sz += varint.Len(len(b.KeyOffsets)) // count
	sz += n                             // body

	n = len(b.ValOffsets) + len(b.Vals)
	sz += varint.Len(n) * 2 // compressed and uncompressed body length
	sz += n                 // body
	return sz
}

func (b *WriteBlock) HasSpace(k, v, blockSize int) bool {
	if len(b.KeyOffsets) == 0 {
		return true
	}
	n := (len(b.KeyOffsets)+1)*2 + len(b.Keys) + k
	sz := varint.Len(n) * 2                 // *2 to estimate compressed length
	sz += varint.Len(len(b.KeyOffsets) + 1) // count
	sz += n                                 // body

	n = (len(b.ValOffsets)+1)*2 + len(b.Vals)
	sz += varint.Len(n) * 2 // compressed and uncompressed body length
	sz += n                 // body
	return sz <= blockSize
}
