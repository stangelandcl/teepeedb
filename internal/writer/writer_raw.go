package writer

import (
	"encoding/binary"
	"io"
	"unsafe"
)

type Raw struct {
	w            io.Writer
	sz, noffsets [10]byte
}

func NewRaw(w io.Writer) *Raw {
	return &Raw{
		w: w,
	}
}

func (w *Raw) WriteBlock(offsets []uint16, body []byte) error {
	offsetsz := binary.PutUvarint(w.noffsets[:], uint64(len(offsets)))
	sz := offsetsz + len(offsets)*2 + len(body)
	nsz := binary.PutUvarint(w.sz[:], uint64(sz))

	_, err := w.w.Write(w.sz[:nsz])
	if err != nil {
		return err
	}
	_, err = w.w.Write(w.noffsets[:offsetsz])
	if err != nil {
		return err
	}
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(&offsets[0])), len(offsets)*2)
	_, err = w.w.Write(bytes)
	if err != nil {
		return err
	}
	_, err = w.w.Write(body)
	if err != nil {
		return err
	}
	return nil
}
