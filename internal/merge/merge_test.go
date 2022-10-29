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

func iterate(filename, f2 string, i, j int) {
	r, _ := reader.NewFile(filename)
	defer r.Close()
	r2, _ := reader.NewFile(f2)
	defer r2.Close()
	c := r.Cursor()
	more := c.First()
	for more {
		kv := c.Current()
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if int(k) != i || int(v) != i {
			log.Panicln("iterate error at", i)
		}
		more = c.Next()
		i++
	}
	r.Close()

	c2 := r2.Cursor()
	more = c2.First()
	for more {
		kv := c2.Current()
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if int(k) != j || int(v) != j {
			log.Panicln("iterate error at", j)
		}
		more = c2.Next()
		j++
	}
	fmt.Println("iterated", i, j)
}

func iterate2(files []string, i int) {
	r, _ := NewReader(files)
	defer r.Close()
	c := r.Cursor()
	more, _ := c.First()
	for more {
		kv := c.Current()
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if int(k) != i || int(v) != i {
			log.Panicln("iterate error at", i, int(k), int(v))
		}
		more, _ = c.Next()
		i++
	}
	fmt.Println("iterated", i)
}

func TestNoOverlap(t *testing.T) {
	os.RemoveAll("test.old.db")
	os.RemoveAll("test.new.db")
	os.RemoveAll("test.db")
	os.RemoveAll("test.db.tmp")
	w := E(writer.NewFile("test.old.db", 16384))

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
	w.Commit()
	w.Close()

	w = E(writer.NewFile("test.new.db", 16384))

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
	w.Commit()
	w.Close()

	//iterate("test.old.db", count)
	//iterate("test.new.db", "test.old.db", 0, count)
	//iterate2([]string{"test.new.db"}, 0)
	//iterate2([]string{"test.new.db", "test.old.db"}, 0)
	//os.Exit(1)

	m, err := NewMerger("test.db", []string{"test.new.db", "test.old.db"}, true, 16384)
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

	w = E(writer.NewFile("test.new.db", 16384))
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
	w.Commit()
	w.Close()

	m, err = NewMerger("test.db", []string{"test.new.db"}, true, 16384)
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

	r := E(NewReader([]string{"test.db"}))
	defer r.Close()
	c := r.Cursor()
	defer c.Close()

	ids := []uint32{}
	i := 0
	more, _ := c.First()
	for more {
		kv = c.Current()
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i < 10 {
			fmt.Println("i", i, "k", k, "v", v)
		}

		more, _ = c.Next()
		ids = append(ids, k)
		i++
	}
	fmt.Println("len keys", len(ids))
}

func TestMerge(t *testing.T) {
	os.RemoveAll("test.old.db")
	os.RemoveAll("test.new.db")
	os.RemoveAll("test.db")
	w := E(writer.NewFile("test.old.db", 16384))

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

	err = w.Commit()
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("wrote", count, "in", time.Since(tm))

	w = E(writer.NewFile("test.new.db", 16384))

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

	err = w.Commit()
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("wrote", 500_000, "in", time.Since(tm))

	r := E(NewReader([]string{"test.new.db", "test.old.db"}))

	tm = time.Now()
	c := r.Cursor()

	i := 0
	more, _ := c.First()
	for more {
		kv = c.Current()
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i < 500_000 && v != math.MaxUint32 {
			log.Panicln("i", i, "v", v)
		} else if i >= 500_000 && (uint32(i) != v || uint32(i) != k) {
			log.Panicln("i", i, "v", v)
		}
		more, _ = c.Next()
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

	buf := make([]byte, 4)
	/*
		tm = time.Now()

		for i := uint32(0); i < count; i++ {
			binary.BigEndian.PutUint32(buf, uint32(i))
			found, _ := c.Get(buf)
			if found {
				kv = c.Current()
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
	*/

	tm = time.Now()
	for i := uint32(0); i < count; i++ {
		binary.BigEndian.PutUint32(buf, uint32(i))
		f, _ := c.Find(buf)
		if f == reader.Found {
			kv = c.Current()
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
	/*
		rand.Shuffle(len(ids), func(i, j int) {
			ids[i], ids[j] = ids[j], ids[i]
		})

		tm = time.Now()
		for _, id := range ids[:1_000_000] {
			binary.BigEndian.PutUint32(buf, uint32(id))
			found, _ := c.Get(buf)
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
			found, _ := c.Get(buf)
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
	*/
	/*
		tm = time.Now()
		for i = 0; i < count; i++ {
			binary.BigEndian.PutUint32(buf, uint32(i))
			f, _ := c.Find(buf)
			if f == reader.Found {
				kv = c.Current()
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
	*/

	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	tm = time.Now()
	for _, id := range ids[:1_000_000] {
		binary.BigEndian.PutUint32(buf, uint32(id))
		f, _ := c.Find(buf)
		if f == reader.Found {
			kv = c.Current()
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
		f, _ := c.Find(buf)
		if f == reader.Found {
			kv = c.Current()
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

	m, err := NewMerger("test.db.tmp", []string{"test.new.db", "test.old.db"}, true, 16384)
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
	r = E(NewReader([]string{"test.db.tmp"}))
	defer r.Close()
	c = r.Cursor()
	defer c.Close()

	i = 0
	more, _ = c.First()
	for more {
		kv = c.Current()
		v := binary.BigEndian.Uint32(kv.Value)
		if i < 500_000 && v != math.MaxUint32 {
			log.Panicln("i", i, "v", v)
		} else if i >= 500_000 && v == math.MaxUint32 {
			log.Panicln("i", i, "v", v)
		}
		more, _ = c.Next()
		i++
	}
	fmt.Println("iterated new file", i, "in", time.Since(tm))
}
