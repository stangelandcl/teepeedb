package teepeedb

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
	db, err := Open("test.db", WithCacheSize(64*1024*1024), WithLz4())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	count := 10_000_000
	w, err := db.Write()
	if err != nil {
		panic(err)
	}
	// always call close whether calling commit or not
	//defer w.Close()
	tm := time.Now()
	for i := 0; i < count; i++ {
		// data is sorted in memcmp/bytes.Compare order so use big-endian
		// when serializing integers as keys to maintain their numeric order
		k := binary.BigEndian.AppendUint32(nil, uint32(i))

		// inserts and deletes must happen in sorted order within a transaction
		// .Add() will fail if bytes.Compare(k, lastKey) <= 0
		err = w.Add(k, k)
		if err != nil {
			panic(err)
		}
	}
	// commit means the data is fsynced safely on disk. It won't show up
	// in a cursor until a new cursor is opened.
	// a merge is immediately triggered on a commit and a reader is reloaded
	// after a merge
	err = w.Commit()
	if err != nil {
		panic(err)
	}
	// readers can read while a writer is open but other writers
	// can't be opened until this writer is closed.
	// single writer, multi-reader
	w.Close()
	fmt.Println("added", count, "in", time.Since(tm))

	tm = time.Now()
	c := db.Cursor()

	// every cursor must be closed because it tracks when it can reload files
	// after a merge or write happened to get the newest data
	defer c.Close()

	kv := KV{}
	i := uint32(0)
	// always call first/last/find before next/previous
	more := c.First(&kv)
	for more {
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i != k || i != v {
			log.Panicln("i", i, "k", k, "v", v)
		}
		more = c.Next(&kv)
		i++
	}
	fmt.Println("iterated", i, "in", time.Since(tm))

	keys := make([]uint32, count)
	for i := 0; i < count; i++ {
		keys[i] = uint32(i)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	tm = time.Now()
	finds := 0
	buf := [4]byte{}
	for i := 0; i < count/10; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		kv.Key = buf[:]
		found := c.Find(&kv)
		switch found {
		case Found:
			//exact match
			finds++
		case FoundGreater:
			fmt.Println("key not found. returned next key-value pair greater than key", binary.BigEndian.Uint32(kv.Key), "vallen", len(kv.Value))
		case NotFound:
			fmt.Println("key not found and no keys greater than key found")
		}
	}

	fmt.Println("found", finds, "keys of", count/10, "in random order in", time.Since(tm))

	stats := db.Stats()
	fmt.Println("estimated total size", stats.Size(), "estimated key count", stats.Count())
}

func E[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}

func TestDB(t *testing.T) {
	os.RemoveAll("test.db")
	db := E(Open("test.db", WithCacheSize(256*1024*1024)))
	defer db.Close()

	var err error
	count := 10_000_000
	w := E(db.Write())
	defer w.Close()
	tm := time.Now()
	for i := 0; i < count; i++ {
		k := binary.BigEndian.AppendUint32(nil, uint32(i))
		err = w.Add(k, k)
		if err != nil {
			panic(err)
		}
		if (i+1)%100_000 == 0 {
			err = w.Commit()
			if err != nil {
				panic(err)
			}
			w.Close()
			w = E(db.Write())
		}
	}
	err = w.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Println("added", count, "in", time.Since(tm))

	tm = time.Now()
	c := db.Cursor()
	defer c.Close()

	kv := KV{}
	i := uint32(0)
	more := c.First(&kv)
	for more {
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i != k || i != v {
			log.Panicln("i", i, "k", k, "v", v)
		}
		more = c.Next(&kv)
		i++
	}
	fmt.Println("iterated", i, "in", time.Since(tm))

	kv.Key = binary.BigEndian.AppendUint32(nil, uint32(25_578))
	if c.Find(&kv) == Found {
		fmt.Println("found", binary.BigEndian.Uint32(kv.Value))

		if c.Next(&kv) {
			fmt.Println("next", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			if c.Next(&kv) {
				fmt.Println("next", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			}
		}
	}

	kv.Key = binary.BigEndian.AppendUint32(nil, uint32(25_578))
	if c.Find(&kv) == Found {
		fmt.Println("found", binary.BigEndian.Uint32(kv.Value))

		if c.Previous(&kv) {
			fmt.Println("prev", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			if c.Previous(&kv) {
				fmt.Println("prev", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			}
		}
	}
}
