package writer

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stangelandcl/teepeedb/internal/shared"
)

func TestWrite(t *testing.T) {
	os.RemoveAll("test.db")
	f, err := NewFile("test.db", 16384)
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
		binary.BigEndian.PutUint32(kv.Value, uint32(i))
		err = f.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	err = f.Commit()
	if err != nil {
		panic(err)
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}
	fs, _ := os.Stat("test.db")
	fmt.Println("compressed", fs.Size(), "in", time.Since(tm))

}
