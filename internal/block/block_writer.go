package block

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/stangelandcl/teepeedb/internal/lz4"
)

type file struct {
	w   io.Writer
	err error
}

func (w file) Error() error {
	return w.err
}

func (w *file) Write(buf []byte) {
	if w.err == nil {
		_, w.err = w.w.Write(buf)
	}
}

type Writer struct {
	uncomp []byte
	comp   []byte
}

func differences(dst []byte, src []uint16) []byte {
	dst = append(dst, make([]byte, len(src)*2)...)
	x := src[0]
	binary.LittleEndian.PutUint16(dst, x)
	for i := 1; i < len(src); i++ {
		y := src[i]
		binary.LittleEndian.PutUint16(dst[i*2:], y-x)
		x = y
	}
	return dst
}

func (w *Writer) compress() {
	bound := lz4.CompressBlockBound(len(w.uncomp))
	w.comp = append(w.comp[:0], make([]byte, bound)...)
	ncomp := lz4.CompressBlock(w.uncomp, w.comp)
	w.comp = w.comp[:ncomp]
}

var ErrEmpty = fmt.Errorf("teepeedb: tried to write empty block")

func (w *Writer) Write(f io.Writer, b *WriteBlock) (Stats, error) {
	s := Stats{}
	if len(b.KeyOffsets) == 0 {
		return s, ErrEmpty
	}

	for _, o := range b.KeyOffsets {
		if o&1 == 0 {
			s.Upserts++
		} else {
			s.Deletes++
		}
	}

	s.FirstKey = append(s.FirstKey, b.KeyAt(0)...)
	s.LastKey = append(s.LastKey, b.KeyAt(len(b.KeyOffsets)-1)...)

	ew := file{w: f}

	tmp := [3 + 10 + 10]byte{}

	// save offsets - differences compress better
	w.uncomp = differences(w.uncomp[:0], b.KeyOffsets)
	w.uncomp = append(w.uncomp, b.Keys...)

	w.compress()

	n := binary.PutUvarint(tmp[:], uint64(len(w.uncomp)))
	n += binary.PutUvarint(tmp[n:], uint64(len(w.comp)))
	n += binary.PutUvarint(tmp[n:], uint64(len(b.KeyOffsets))) // count
	ew.Write(tmp[:n])                                          // sizes
	ew.Write(w.comp)                                           // compressed bytes

	w.uncomp = differences(w.uncomp[:0], b.ValOffsets)
	w.uncomp = append(w.uncomp, b.Vals...)

	w.compress()

	n = binary.PutUvarint(tmp[:], uint64(len(w.uncomp)))
	n += binary.PutUvarint(tmp[n:], uint64(len(w.comp)))
	ew.Write(tmp[:n]) // sizes
	ew.Write(w.comp)  // compressed bytes

	b.KeyOffsets = b.KeyOffsets[:0]
	b.ValOffsets = b.ValOffsets[:0]
	b.Keys = b.Keys[:0]
	b.Vals = b.Vals[:0]

	return s, ew.Error()
}
