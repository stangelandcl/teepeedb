package reader

import "github.com/stangelandcl/teepeedb/varint"

type Raw struct {
	buf []byte
}

func NewRaw(buf []byte) (*Raw, error) {
	return &Raw{
		buf: buf,
	}, nil
}

func (r *Raw) ReadBlock(pos int) ([]byte, error) {
	sz := varint.Read(r.buf, &pos)
	return r.buf[pos : pos+sz], nil
}
