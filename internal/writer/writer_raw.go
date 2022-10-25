package writer

import (
	"encoding/binary"
	"io"
)

type Raw struct {
	w   io.Writer
	tmp [10]byte
}

func NewRaw(w io.Writer) *Raw {
	return &Raw{
		w: w,
	}
}

func (w *Raw) WriteBlock(blockParts ...[]byte) error {
	sz := 0
	for i := range blockParts {
		sz += len(blockParts[i])
	}

	n := binary.PutUvarint(w.tmp[:], uint64(uint(sz)))

	_, err := w.w.Write(w.tmp[:n])
	if err != nil {
		return err
	}
	for _, part := range blockParts {
		_, err := w.w.Write(part)
		if err != nil {
			return err
		}
	}
	return nil
}
