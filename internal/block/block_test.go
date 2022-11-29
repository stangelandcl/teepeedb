package block

import (
	"bytes"
	"encoding/binary"
	"log"
	"testing"
)

func TestBlock(t *testing.T) {
	w := WriteBlock{}
	for i := 0; i < 200; i++ {
		k := binary.BigEndian.AppendUint32(nil, uint32(i))
		w.Put(k, k, i%7 == 0)
	}

	buf := bytes.Buffer{}
	wr := Writer{}
	wr.Write(&buf, &w)

	r := Read(buf.Bytes())
	defer r.Close()

	for i := 0; i < 200; i++ {
		key, delete := r.Key(i)
		val := r.Value(i)

		kk := int(binary.BigEndian.Uint32(key))
		vv := int(binary.BigEndian.Uint32(val))

		if delete != (i%7 == 0) {
			log.Panicln("bad delete", delete)
		}
		if kk != i || vv != i {
			log.Panicln(i, kk, vv)
		}
	}
}
