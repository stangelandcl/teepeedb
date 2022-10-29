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
		k, del := r.Key(i)
		v := r.Value(i)

		kk := int(binary.BigEndian.Uint32(k))
		vv := int(binary.BigEndian.Uint32(v))

		if del != (i%7 == 0) {
			log.Panicln("bad delete", del)
		}
		if kk != i || vv != i {
			log.Panicln(i, kk, vv)
		}
	}
}
