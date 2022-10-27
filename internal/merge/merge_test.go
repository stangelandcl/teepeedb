package merge

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

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

func TestNoOverlap(t *testing.T) {
	os.RemoveAll("test.old.db")
	os.RemoveAll("test.new.db")
	os.RemoveAll("test.db")
	os.RemoveAll("test.db.tmp")
	cache := reader.NewCache(256 * 1024 * 1024 / 4096)
	w := E(writer.NewFile("test.old.db", 16384, -1, shared.Lz4))

	count := 100_000
	kv := shared.KV{}
	for i := count; i < count*4; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, uint32(i))
		err := w.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	w.Close()

	w = E(writer.NewFile("test.new.db", 16384, -1, shared.Lz4))

	var err error
	count = 100_000
	kv = shared.KV{}
	for i := 0; i < count; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, uint32(i))
		err = w.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	w.Close()

	m, err := NewMerger("test.db", []string{"test.new.db", "test.old.db"}, cache, true, 16384, -1, shared.Lz4)
	if err != nil {
		panic(err)
	}
	err = m.Run()
	if err != nil {
		panic(err)
	}
	err = m.Commit()
	if err != nil {
		panic(err)
	}
	m.Close()

	w = E(writer.NewFile("test.new.db", 16384, -1, shared.Lz4))
	count = 100_000
	kv = shared.KV{}
	for i := count * 10; i < count*11; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, uint32(i))
		err = w.Add(&kv)
		if err != nil {
			panic(err)
		}
	}
	w.Close()

	m, err = NewMerger("test.db", []string{"test.new.db"}, cache, true, 16384, -1, shared.Lz4)
	if err != nil {
		panic(err)
	}
	err = m.Run()
	if err != nil {
		panic(err)
	}
	err = m.Commit()
	if err != nil {
		panic(err)
	}
	m.Close()

	r := E(NewReader([]string{"test.db"}, cache))
	defer r.Close()
	c := r.Cursor()
	defer c.Close()

	ids := []uint32{}
	i := 0
	more := c.First(&kv)
	for more {
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i < 10 {
			fmt.Println("i", i, "k", k, "v", v)
		}

		more = c.Next(&kv)
		ids = append(ids, k)
		i++
	}
	fmt.Println("len keys", len(ids))
}

func TestMerge(t *testing.T) {
	os.RemoveAll("test.old.db")
	os.RemoveAll("test.new.db")
	os.RemoveAll("test.db")
	cache := reader.NewCache(256 * 1024 * 1024 / 4096)
	w := E(writer.NewFile("test.old.db", 16384, -1, shared.Lz4))

	var err error
	tm := time.Now()
	const count = 10_000_000
	kv := shared.KV{}
	for i := 0; i < count; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, uint32(i))
		err = w.Add(&kv)
		if err != nil {
			panic(err)
		}
	}

	err = w.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("wrote", count, "in", time.Since(tm))

	w = E(writer.NewFile("test.new.db", 16384, -1, shared.Lz4))

	tm = time.Now()
	for i := 0; i < 500_000; i++ {
		kv.Key = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Key, uint32(i))
		kv.Value = make([]byte, 4)
		binary.BigEndian.PutUint32(kv.Value, uint32(math.MaxUint32))
		err = w.Add(&kv)
		if err != nil {
			panic(err)
		}
	}

	err = w.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("wrote", 500_000, "in", time.Since(tm))

	r := E(NewReader([]string{"test.new.db", "test.old.db"}, cache))

	tm = time.Now()
	c := r.Cursor()

	i := 0
	more := c.First(&kv)
	for more {
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i < 500_000 && v != math.MaxUint32 {
			log.Panicln("i", i, "v", v)
		} else if i >= 500_000 && (uint32(i) != v || uint32(i) != k) {
			log.Panicln("i", i, "v", v)
		}
		more = c.Next(&kv)
		i++
	}
	fmt.Println("iterated", i, "in", time.Since(tm))

	/*
		i = count - 1
		more = c.Last(&kv)
		for more {
			v := binary.BigEndian.Uint32(kv.Value)
			if i < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", i, "v", v)
			} else if i >= 500_000 && v == math.MaxUint32 {
				log.Panicln("i", i, "v", v)
			}
			more = c.Previous(&kv)
			i--
		}
		fmt.Println("reversed", count, "in", time.Since(tm))
	*/
	//c = r.Cursor()

	tm = time.Now()
	buf := make([]byte, 4)
	for i := uint32(0); i < count; i++ {
		binary.BigEndian.PutUint32(buf, uint32(i))
		kv.Key = buf
		if c.Get(&kv) {
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if i < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", i, "v", v)
			} else if i >= 500_000 && (i != v || i != k) {
				log.Panicln("i", i, "v", v)
			}
		} else {
			log.Panicln("missing find", i)
		}
	}
	fmt.Println("get all sorted", count, "in", time.Since(tm))

	tm = time.Now()
	for i := uint32(0); i < count; i++ {
		binary.BigEndian.PutUint32(buf, uint32(i))
		kv.Key = buf
		if c.Find(&kv) == reader.Found {
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if i < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", i, "v", v)
			} else if i >= 500_000 && (i != v || i != k) {
				log.Panicln("i", i, "v", v)
			}
		} else {
			log.Panicln("missing find", i)
		}
	}
	fmt.Println("find all sorted", count, "in", time.Since(tm))

	ids := make([]uint32, count)
	for i = 0; i < count; i++ {
		ids[i] = uint32(i)
	}
	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	tm = time.Now()
	for _, id := range ids[:1_000_000] {
		binary.BigEndian.PutUint32(buf, uint32(id))
		kv.Key = buf
		if c.Get(&kv) {
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if id < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", id, "v", v)
			} else if id >= 500_000 && (id != v || id != k) {
				log.Panicln("i", id, "v", v)
			}
		} else {
			log.Panicln("missing find", id)
		}
	}
	fmt.Println("get rand", 1_000_000, "in", time.Since(tm))

	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	tm = time.Now()
	for _, id := range ids[:1_000_000] {
		binary.BigEndian.PutUint32(buf, uint32(id))
		kv.Key = buf
		found := c.Get(&kv)
		if found {
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if id < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", id, "v", v)
			} else if id >= 500_000 && (id != v || id != k) {
				log.Panicln("i", id, "v", v)
			}
		} else {
			log.Panicln("missing find", id)
		}
	}
	fmt.Println("get rand sorted", 1_000_000, "in", time.Since(tm))

	tm = time.Now()
	for i = 0; i < count; i++ {
		binary.BigEndian.PutUint32(buf, uint32(i))
		kv.Key = buf
		if c.Find(&kv) == reader.Found {
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if i < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", i, "v", v, "k", k)
			} else if i >= 500_000 && v == math.MaxUint32 {
				log.Panicln("i", i, "v", v, "k", k)
			}
		} else {
			log.Panicln("missing find", i)
		}
	}
	fmt.Println("find all sorted", i, "in", time.Since(tm))

	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	tm = time.Now()
	for _, id := range ids[:1_000_000] {
		binary.BigEndian.PutUint32(buf, uint32(id))
		kv.Key = buf
		if c.Find(&kv) == reader.Found {
			v := binary.BigEndian.Uint32(kv.Value)
			if id < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", id, "v", v)
			} else if id >= 500_000 && v == math.MaxUint32 {
				log.Panicln("i", id, "v", v)
			}
		} else {
			log.Panicln("missing find", id)
		}
	}
	fmt.Println("find rand unsorted", 1_000_000, "in", time.Since(tm))

	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	tm = time.Now()
	for _, id := range ids[:1_000_000] {
		binary.BigEndian.PutUint32(buf, uint32(id))
		kv.Key = buf
		if c.Find(&kv) == reader.Found {
			v := binary.BigEndian.Uint32(kv.Value)
			if id < 500_000 && v != math.MaxUint32 {
				log.Panicln("i", id, "v", v)
			} else if id >= 500_000 && v == math.MaxUint32 {
				log.Panicln("i", id, "v", v)
			}
		} else {
			log.Panicln("missing find", id)
		}
	}
	fmt.Println("find rand sorted", 1_000_000, "in", time.Since(tm))

	tm = time.Now()

	m, err := NewMerger("test.db.tmp", []string{"test.new.db", "test.old.db"}, cache, true, 16384, -1, shared.Lz4)
	if err != nil {
		panic(err)
	}
	err = m.Run()
	if err != nil {
		panic(err)
	}
	err = m.Commit()
	if err != nil {
		panic(err)
	}
	m.Close()
	c.Close()
	r.Close()
	r = E(NewReader([]string{"test.db.tmp"}, cache))
	defer r.Close()
	c = r.Cursor()
	defer c.Close()

	i = 0
	more = c.First(&kv)
	for more {
		v := binary.BigEndian.Uint32(kv.Value)
		if i < 500_000 && v != math.MaxUint32 {
			log.Panicln("i", i, "v", v)
		} else if i >= 500_000 && v == math.MaxUint32 {
			log.Panicln("i", i, "v", v)
		}
		more = c.Next(&kv)
		i++
	}
	fmt.Println("iterated new file", i, "in", time.Since(tm))
}
