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
		// key < int16 because 1 bit is used for delete flag
		// values < uint16 because all bits are used for size
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

// index = 1 for index block
// index = 0 for data block
// higher level must have already checked that keylen <= shared.MaxKeySize
func (b *WriteBlock) HasSpace(k, v, blockSize int, index int) bool {
	// data block can get by with only 1 key but
	// index blocks need two keys to make progress
	// and not continually pushing the index up to a higher level
	// index until it runs out of memory
	// A higher level than this should check that key is <= shared.MaxKeySize
	if len(b.KeyOffsets) <= index {
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
