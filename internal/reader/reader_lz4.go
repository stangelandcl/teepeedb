package reader

import (
	"encoding/binary"
	"log"
	"sync/atomic"

	"github.com/stangelandcl/teepeedb/internal/lz4"
	"github.com/stangelandcl/teepeedb/internal/varint"
)

type Lz4 struct {
	buf   []byte
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
		id:    atomic.AddInt64(&id, 1),
		buf:   buf,
		cache: cache,
	}
	if r.cache == nil {
		r.cache = &NullCache{}
	}
	return r, nil
}

func (r *Lz4) ReadBlock(pos int) []byte {
	key := hashKey{
		Id:       r.id,
		Position: pos,
	}
	bufi, ok := r.cache.Get(key)
	var decomp []byte
	if ok {
		decomp = bufi.([]byte)
	} else {
		uncompSz := varint.Read(r.buf, &pos)
		compSz := varint.Read(r.buf, &pos)
		buf := r.buf[pos : pos+compSz]
		decomp = make([]byte, uncompSz)
		n := lz4.UncompressBlock(buf, decomp)
		if n != uncompSz {
			log.Panicln("decompressed does not match block size got", n, "expected", uncompSz)
		}
		pos = 0
		noffsets := varint.Read(decomp, &pos) * 2
		x := binary.LittleEndian.Uint16(decomp[pos:])
		pos += 2
		end := noffsets - 2 + pos
		for i := pos; i < end; i += 2 {
			y := binary.LittleEndian.Uint16(decomp[i:])
			x += y
			binary.LittleEndian.PutUint16(decomp[i:], x)
		}

		r.cache.Add(key, decomp)
	}

	return decomp
}
