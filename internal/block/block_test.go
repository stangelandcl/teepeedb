package block

import (
	"bytes"
	"encoding/binary"
	"log"
	"testing"

	"github.com/stangelandcl/teepeedb/internal/shared"
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
		kv := shared.KV{}
		r.At(i, Both, &kv)

		kk := int(binary.BigEndian.Uint32(kv.Key))
		vv := int(binary.BigEndian.Uint32(kv.Value))

		if kv.Delete != (i%7 == 0) {
			log.Panicln("bad delete", kv.Delete)
		}
		if kk != i || vv != i {
			log.Panicln(i, kk, vv)
		}
	}
}
