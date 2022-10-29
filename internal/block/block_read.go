package block

import (
	"encoding/binary"
	"log"
	"sync"

	"github.com/stangelandcl/teepeedb/internal/lz4"
)

type ReadBlock struct {
	KeyOffsets, ValOffsets []uint16
	Keys                   []byte
	Vals                   []byte
	Count                  int

	// remaining compressed value bytes
	// directly references mmapped file
	vbuf []byte

	// uncompressed buffers
	// for reusing slice memory so each decompression
	// doesn't have to allocate
	kuncomp []byte
	vuncomp []byte
}

var pool = sync.Pool{}

func (b *ReadBlock) Close() {
	b.KeyOffsets = b.KeyOffsets[:0]
	b.ValOffsets = b.ValOffsets[:0]
	b.Keys = b.Keys[:0]
	b.Vals = b.Vals[:0]
	b.vbuf = nil
	b.kuncomp = b.kuncomp[:0]
	b.vuncomp = b.vuncomp[:0]
	b.Count = 0
	pool.Put(b)
}

func (b ReadBlock) KeyOffset(idx int) (offset int, delete bool) {
	x := int(b.KeyOffsets[idx])
	offset = x >> 1
	delete = x&1 != 0
	return
}

func (b ReadBlock) Key(idx int) (key []byte, delete bool) {
	var start, end int
	start, delete = b.KeyOffset(idx)
	idx++
	if idx == int(b.Count) {
		end = len(b.Keys)
	} else {
		end, _ = b.KeyOffset(idx)
	}
	key = b.Keys[start:end]
	return
}

func (b *ReadBlock) Value(idx int) []byte {
	if len(b.ValOffsets) == 0 {
		b.value()
	}

	var start, end int
	start = int(b.ValOffsets[idx])
	idx++
	if idx == int(b.Count) {
		end = len(b.Vals)
	} else {
		end = int(b.ValOffsets[idx])
	}
	return b.Vals[start:end]
}

func offsets(dst []uint16, src []byte, n int) []uint16 {
	x := binary.LittleEndian.Uint16(src)
	dst = append(dst, x)
	// deserialize remaining differences into offsets
	for i := 1; i < n; i++ {
		y := binary.LittleEndian.Uint16(src[i*2:])
		x += y
		dst = append(dst, x)
	}
	return dst
}

func (r *ReadBlock) uncompress(dst, comp []byte, nuncomp int) []byte {
	dst = append(dst, make([]byte, nuncomp)...)
	n := lz4.UncompressBlock(comp, dst)
	if n != nuncomp {
		log.Panicln("lz4 uncompressed to wrong size:", n)
	}
	return dst
}

//var poolcount = 0

func Read(buf []byte) *ReadBlock {
	nuncomp, n := binary.Uvarint(buf)
	buf = buf[n:]
	ncomp, n := binary.Uvarint(buf)
	buf = buf[n:]
	count, n := binary.Uvarint(buf)
	buf = buf[n:]

	comp := buf[:ncomp]
	buf = buf[ncomp:]

	r, ok := pool.Get().(*ReadBlock)
	if !ok {
		//poolcount++
		//fmt.Println("missing from pool", poolcount)
		r = &ReadBlock{}
	}

	r.kuncomp = r.uncompress(r.kuncomp[:0], comp, int(nuncomp))
	r.Count = int(count)
	r.KeyOffsets = offsets(r.KeyOffsets[:0], r.kuncomp, r.Count)
	r.Keys = r.kuncomp[r.Count*2:]
	r.vbuf = buf
	r.Vals = r.Vals[:0]
	r.ValOffsets = r.ValOffsets[:0]
	return r
}

func (r *ReadBlock) value() {
	buf := r.vbuf
	nuncomp, n := binary.Uvarint(buf)
	buf = buf[n:]
	ncomp, n := binary.Uvarint(buf)
	buf = buf[n:]
	comp := buf[:ncomp]

	r.vuncomp = r.uncompress(r.vuncomp[:0], comp, int(nuncomp))
	r.ValOffsets = offsets(r.ValOffsets[:0], r.vuncomp, r.Count)
	r.Vals = r.vuncomp[r.Count*2:]
}
