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

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/writer"
)

func E[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}
func TestFile(t *testing.T) {
	for i := 0; i < 1; i++ {
		run()
	}
	novalues()
}

func novalues() {
	w, err := writer.NewFile("test.db", 4096)
	if err != nil {
		panic(err)
	}

	tm := time.Now()
	count := 10_000_000
	kv := shared.KV{}
	for i := 0; i < count; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = nil
		err = w.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	err = w.Commit()
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	fs, _ := os.Stat("test.db")
	fmt.Println("wrote no values", count, "in", time.Since(tm), "len", fs.Size())

	r, err := reader.NewFile("test.db")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	tm = time.Now()
	c := r.Cursor()
	i := uint32(0)
	if !c.First() {
		panic("not first")
	}
	for {
		kv := shared.KV{}
		c.Current(block.Both, &kv)
		if len(kv.Key) != 4 {
			panic("len != 4")
		}
		k := binary.BigEndian.Uint32(kv.Key)
		if k != i || len(kv.Value) != 0 {
			log.Panicln("invalid kv at", i, k, len(kv.Value))
		}
		i++
		if !c.Next() {
			break
		}
	}
	if int(i) != count {
		log.Panicln("i != count", i, count)
	}
	fmt.Println("forward no values", i, "in", time.Since(tm))
}

func run() {
	w, err := writer.NewFile("test.db", 4096)
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
	err = w.Commit()
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	fs, _ := os.Stat("test.db")
	fmt.Println("wrote", count, "in", time.Since(tm), "len", fs.Size())

	r, err := reader.NewFile("test.db")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	tm = time.Now()
	c := r.Cursor()
	i := uint32(0)
	if !c.First() {
		panic("not first")
	}
	for {
		kv := shared.KV{}
		c.Current(block.Both, &kv)
		if len(kv.Key) != 4 || len(kv.Value) != 4 {
			panic("len != 4")
		}
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if k != i || v != 1 {
			log.Panicln("invalid kv at", i, k)
		}
		i++
		if !c.Next() {
			break
		}
	}
	if int(i) != count {
		log.Panicln("i != count", i, count)
	}
	fmt.Println("forward", i, "in", time.Since(tm))

	i = uint32(count)
	if !c.Last() {
		panic("not last")
	}
	for {
		kv := shared.KV{}
		c.Current(block.Both, &kv)
		i--
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if k != i || v != 1 {
			log.Panicln("invalid kv at", i, k)
		}
		if !c.Previous() {
			break
		}
	}
	if int(i) != 0 {
		log.Panicln("i != 0", i, 0)
	}
	fmt.Println("backward", count, "in", time.Since(tm))

	for i := uint32(0); i < uint32(count); i++ {
		buf := [4]byte{}
		binary.BigEndian.PutUint32(buf[:], i)
		if c.Find(buf[:]) == 0 {
			log.Panicln("can't find sorted", i)
		}
		kv := shared.KV{}
		c.Current(block.Both, &kv)
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
		buf := [4]byte{}
		binary.BigEndian.PutUint32(buf[:], ids[i])
		if c.Find(buf[:]) == 0 {
			log.Panicln("can't find", ids[i], "at", i)
		}
		kv := shared.KV{}
		c.Current(block.Both, &kv)
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
		buf := [4]byte{}
		binary.BigEndian.PutUint32(buf[:], ids[i])
		if c.Find(buf[:]) == 0 {
			log.Panicln("can't find", ids[i], "at", i)
		}
		kv := shared.KV{}
		c.Current(block.Both, &kv)
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if k != ids[i] || v != 1 {
			log.Panicln("invalid kv at", i, k, v, kv.Key, kv.Value)
		}
	}
	fmt.Println("find rand sorted", 1_000_000, "in", time.Since(tm))
}
