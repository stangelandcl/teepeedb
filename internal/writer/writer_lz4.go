package writer

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pierrec/lz4/v4"
)

type Lz4 struct {
	compressed bytes.Buffer
	zw         *lz4.Writer
	w          *Raw
}

func NewLz4(wr io.Writer) *Lz4 {
	raw := NewRaw(wr)
	w := &Lz4{
		w: raw,
	}
	w.zw = lz4.NewWriter(&w.compressed)
	return w

}

func (w *Lz4) WriteBlock(blockParts ...[]byte) error {
	uncompSz := 0
	for i := range blockParts {
		uncompSz += len(blockParts[i])
	}

	w.compressed.Reset()
	w.zw.Reset(&w.compressed)

	for _, part := range blockParts {
		_, err := w.zw.Write(part)
		if err != nil {
			return err
		}
	}

	err := w.zw.Close()
	if err != nil {
		return err
	}

	tmp := make([]byte, 0, 10)
	tmp = binary.AppendUvarint(tmp, uint64(uint(uncompSz)))

	return w.w.WriteBlock(tmp, w.compressed.Bytes())
}
