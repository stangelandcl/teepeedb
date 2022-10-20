package writer

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stangelandcl/teepeedb/shared"
)

func TestWrite(t *testing.T) {
	opt := NewOpt()
	opt.BlockSize = 16384
	f, err := NewFile("test.db", opt)
	if err != nil {
		panic(err)
	}

	tm := time.Now()
	count := 100_000_000
	kv := shared.KV{}
	for i := 0; i < count; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, 0)
		err = f.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}
	fs, _ := os.Stat("test.db")
	fmt.Println("uncompressed", fs.Size(), "in", time.Since(tm))

	opt.Compressed = true
	f, err = NewFile("test.db", opt)
	if err != nil {
		panic(err)
	}

	tm = time.Now()
	count = 100_000_000
	kv = shared.KV{}
	for i := 0; i < count; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, 0)
		err = f.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}
	fs, _ = os.Stat("test.db")
	fmt.Println("compressed", fs.Size(), "in", time.Since(tm))

}
