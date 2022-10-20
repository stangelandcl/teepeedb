package test

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/shared"
	"github.com/stangelandcl/teepeedb/writer"
)

func TestFile(t *testing.T) {
	cache := reader.NewCache(256 * 1024 * 1024 / 4096)
	for u := 0; u < 2; u++ {
		opt := writer.NewOpt()
		opt.Compressed = u == 1
		fmt.Println()
		fmt.Println("compressed?", opt.Compressed)
		w, err := writer.NewFile("test.db", opt)
		if err != nil {
			panic(err)
		}

		tm := time.Now()
		count := 10_000_000
		kv := shared.KV{}
		for i := 0; i < count; i++ {
			kv.Key = make([]byte, 4)
			binary.BigEndian.PutUint32(kv.Key, uint32(i))
			kv.Value = make([]byte, 4)
			binary.BigEndian.PutUint32(kv.Value, uint32(1))
			err = w.Add(&kv)
			if err != nil {
				panic(err)
			}
		}

		err = w.Close()
		if err != nil {
			panic(err)
		}
		fs, _ := os.Stat("test.db")
		fmt.Println("wrote", count, "in", time.Since(tm), "len", fs.Size())

		r, err := reader.NewFile("test.db", cache)
		if err != nil {
			panic(err)
		}
		defer r.Close()

		tm = time.Now()
		c, err := r.Cursor()
		if err != nil {
			panic(err)
		}
		i := uint32(0)
		if !c.First(&kv) {
			panic("not first")
		}
		for {
			if len(kv.Key) != 4 || len(kv.Value) != 4 {
				panic("len != 4")
			}
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if k != i || v != 1 {
				log.Panicln("invalid kv at", i, k)
			}
			i++
			if !c.Next(&kv) {
				break
			}
		}
		if int(i) != count {
			log.Panicln("i != count", i, count)
		}
		fmt.Println("forward", i, "in", time.Since(tm))

		i = uint32(count)
		if !c.Last(&kv) {
			panic("not last")
		}
		for {
			i--
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if k != i || v != 1 {
				log.Panicln("invalid kv at", i, k)
			}
			if !c.Previous(&kv) {
				break
			}
		}
		if int(i) != 0 {
			log.Panicln("i != 0", i, 0)
		}
		fmt.Println("backward", count, "in", time.Since(tm))

		for i := uint32(0); i < uint32(count); i++ {
			kv.Key = make([]byte, 4)
			binary.BigEndian.PutUint32(kv.Key, i)
			if c.Find(&kv) == 0 {
				log.Panicln("can't find sorted", i)
			}
			if len(kv.Key) != 4 || len(kv.Value) != 4 {
				panic("len != 4")
			}
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if k != i || v != 1 {
				log.Panicln("invalid kv at", i, k, v, kv.Key, kv.Value)
			}
		}
		fmt.Println("findsorted", count, "in", time.Since(tm))

		ids := make([]uint32, count)
		for i := range ids {
			ids[i] = uint32(i)
		}
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		tm = time.Now()
		for i := uint32(0); i < 1_000_000; i++ {
			kv.Key = make([]byte, 4)
			binary.BigEndian.PutUint32(kv.Key, ids[i])
			if c.Find(&kv) == 0 {
				log.Panicln("can't find", ids[i], "at", i)
			}
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if k != ids[i] || v != 1 {
				log.Panicln("invalid kv at", i, k, v, kv.Key, kv.Value)
			}
		}
		fmt.Println("findrand", 1_000_000, "in", time.Since(tm))

		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

		sort.Slice(ids[:1_000_000], func(i, j int) bool { return ids[i] < ids[j] })
		tm = time.Now()
		for i := uint32(0); i < 1_000_000; i++ {
			kb := make([]byte, 4)
			binary.BigEndian.PutUint32(kb, ids[i])
			kv.Key = kb
			if c.Find(&kv) == 0 {
				log.Panicln("can't find", ids[i], "at", i)
			}
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if k != ids[i] || v != 1 {
				log.Panicln("invalid kv at", i, k, v, kv.Key, kv.Value)
			}
		}
		fmt.Println("find rand sorted", 1_000_000, "in", time.Since(tm))
	}
}
