package writer

import (
	"encoding/binary"
	"io"
	"log"

	"github.com/stangelandcl/teepeedb/internal/lz4"
	"github.com/stangelandcl/teepeedb/internal/varint"
)

type Lz4 struct {
	w             io.Writer
	uncomp, compr []byte
	tmp           [16]byte
}

func NewLz4(w io.Writer) *Lz4 {
	return &Lz4{w: w}
}

func (w *Lz4) WriteBlock(offsets []uint16, body []byte) error {
	n := varint.Len(len(offsets)) + len(offsets)*2 + len(body)
	w.uncomp = append(w.uncomp[:0], make([]byte, n)...)

	pos := binary.PutUvarint(w.uncomp, uint64(len(offsets)))
	// differences compress better
	x := offsets[0]
	pos += 2
	for i := 1; i < len(offsets); i++ {
		y := offsets[i]
		binary.LittleEndian.PutUint16(w.uncomp[pos:], y-x)
		pos += 2
		x = y
	}
	pos += copy(w.uncomp[pos:], body)
	if len(w.uncomp) != pos {
		log.Panicln("lz4 block writer: pos mismatch", len(w.uncomp), "!=", pos)
	}

	uncompSz := len(w.uncomp)
	bound := lz4.CompressBlockBound(uncompSz)
	w.compr = append(w.compr[:0], make([]byte, bound)...)
	compsz := lz4.CompressBlock(w.uncomp, w.compr[:bound])
	comp := w.compr[:compsz]

	n = binary.PutUvarint(w.tmp[:], uint64(uncompSz))
	n += binary.PutUvarint(w.tmp[n:], uint64(compsz))
	_, err := w.w.Write(w.tmp[:n])
	if err != nil {
		return err
	}
	_, err = w.w.Write(comp)
	return err
}
