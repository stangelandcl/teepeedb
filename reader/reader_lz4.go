package reader

import (
	"bytes"
	"sync/atomic"

	"github.com/pierrec/lz4/v4"
	"github.com/stangelandcl/teepeedb/varint"
)

type Lz4 struct {
	raw   *Raw
	r     *bytes.Reader
	zr    *lz4.Reader
	cache Cache
	id    int64
}

type hashKey struct {
	Id       int64
	Position int
}

var id int64

func NewLz4(buf []byte, cache Cache) (*Lz4, error) {
	r := &Lz4{
		id: atomic.AddInt64(&id, 1),
	}
	raw, err := NewRaw(buf)
	if err != nil {
		return nil, err
	}
	r.raw = raw
	r.zr = lz4.NewReader(nil)
	r.r = bytes.NewReader(nil)
	r.cache = cache
	if r.cache == nil {
		r.cache = &NullCache{}
	}
	return r, nil
}

func (r *Lz4) ReadBlock(pos int) ([]byte, error) {
	key := hashKey{
		Id:       r.id,
		Position: pos,
	}
	bufi, ok := r.cache.Get(key)
	var decomp []byte
	if ok {
		decomp = bufi.([]byte)
	} else {
		buf, err := r.raw.ReadBlock(pos)
		if err != nil {
			return nil, err
		}
		i := 0
		uncompSz := varint.Read(buf, &i)
		buf = buf[i:]
		r.r.Reset(buf)
		r.zr.Reset(r.r)
		decomp = make([]byte, uncompSz)
		_, err = r.zr.Read(decomp)
		if err != nil {
			return nil, err
		}
		r.cache.Add(key, decomp)
	}

	return decomp, nil
}
