package reader

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/stangelandcl/teepeedb/shared"
	"github.com/stangelandcl/teepeedb/varint"
)

type Move byte
type FindResult byte

const (
	// no values greater or equal to key exist
	NotFound FindResult = 0
	// exact match
	Found FindResult = 1
	// found a value greater or equal to key
	Partial FindResult = 2
)

const (
	First    Move = 0
	Last     Move = 1
	Next     Move = 2
	Previous Move = 3
)

// readers are lightweight and can be recreated for each block read
type Block struct {
	buf       []byte
	count     int32
	idx       int32 // set to next value to read in forward iter order
	fixedSize int   // fixed valueSize < 0 == variable length
}

// readers are lightweight and can be recreated for each block read
// fixedValueSize < 0 == variable length
// every reader needs its own decompressor
func NewBlock(buf []byte, fixedValueSize int) Block {
	b := Block{
		fixedSize: fixedValueSize,
		idx:       -1,
	}
	pos := 0
	b.count = int32(varint.Read(buf, &pos))
	b.buf = buf[pos:]
	return b
}

func (b *Block) Find(kv *shared.KV, back bool) FindResult {
	lo := int32(0)
	hi := b.count - 1

	for lo <= hi {
		i := (lo + hi) / 2

		p := int(binary.LittleEndian.Uint16(b.buf[i*2:]))
		p += int(b.count) * 2
		ks := varint.Read(b.buf, &p)
		delete := ks&1 != 0
		ks >>= 1
		key := b.buf[p : p+ks]
		c := bytes.Compare(key, kv.Key)
		if c < 0 {
			lo = i + 1
		} else if c > 0 {
			hi = i - 1
		} else {
			kv.Key = key
			kv.Delete = delete
			p += ks
			vs := b.fixedSize
			if vs < 0 {
				vs = varint.Read(b.buf, &p)
			}
			kv.Value = b.buf[p : p+vs]
			b.idx = i
			return Found
		}
	}

	/* return first value less than key */
	b.idx = lo
	if back {
		if b.idx > 0 {
			b.idx--
		}
		b.read(kv)
		return Partial
	} else if b.idx < b.count {
		b.read(kv)
		return Partial
	}
	return NotFound
}

func (b *Block) At(idx int) (key []byte, val []byte, delete bool) {
	offset := int(binary.LittleEndian.Uint16(b.buf))
	offset += int(b.count) * 2

	sz := varint.Read(b.buf, &offset)
	delete = sz&1 != 0
	sz >>= 1
	key = b.buf[offset : offset+sz]
	offset += sz
	sz = b.fixedSize
	if sz < 0 {
		sz = varint.Read(b.buf, &offset)
	}
	val = b.buf[offset : offset+sz]
	return
}

func (b *Block) Len() int {
	return int(b.count)
}

func (b *Block) InRange(kv *shared.KV) bool {
	offset := int(binary.LittleEndian.Uint16(b.buf))
	offset += int(b.count) * 2

	ks := varint.Read(b.buf, &offset) >> 1
	k := b.buf[offset : offset+ks]
	if bytes.Compare(kv.Key, k) < 0 {
		return false
	}
	offset = int(binary.LittleEndian.Uint16(b.buf[b.count*2-2:]))
	offset += int(b.count) * 2

	ks = varint.Read(b.buf, &offset) >> 1
	k = b.buf[offset : offset+ks]

	return bytes.Compare(kv.Key, k) <= 0
}

func (b *Block) read(kv *shared.KV) {
	offset := int(binary.LittleEndian.Uint16(b.buf[b.idx*2:]))
	offset += int(b.count * 2)

	ks := varint.Read(b.buf, &offset)
	kv.Delete = ks&1 != 0
	ks >>= 1
	kv.Key = b.buf[offset : offset+ks]
	offset += ks

	sz := b.fixedSize
	if sz < 0 {
		sz = varint.Read(b.buf, &offset)
	}
	kv.Value = b.buf[offset : offset+sz]
}

func (b *Block) GoBack() {
	if b.idx > 0 {
		b.idx--
	}
}

func (b *Block) Move(m Move, kv *shared.KV) bool {
	switch m {
	case First:
		b.idx = -1
		fallthrough
	case Next:
		more := b.idx+1 < b.count
		if more {
			b.idx++
			b.read(kv)
			//b.idx++
		}
		return more
	case Last:
		b.idx = b.count
		fallthrough
	case Previous:
		more := b.idx != 0
		if more {
			b.idx--
			b.read(kv)
		}
		return more
	}
	return false
}

func (r *Block) Print() {
	kv := shared.KV{}
	i := r.idx
	if i > 0 {
		r.idx--
		r.read(&kv)
		print(&kv, r.idx, '-')
		r.idx = i
	}
	if r.idx < r.count {
		r.read(&kv)
		print(&kv, r.idx, '*')
	}
	if r.idx < r.count-1 {
		r.idx++
		r.read(&kv)
		print(&kv, r.idx, '+')
	}
	r.idx = i
}

func print(kv *shared.KV, idx int32, hit byte) {
	fmt.Println("k", kv.Key, "v", kv.Value, "i", idx, string(hit))
}
