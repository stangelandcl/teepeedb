package db

import (
	"encoding/binary"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/shared"
)

func E[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}

func TestDB(t *testing.T) {
	db := E(Open("test.db", NewOpt().WithCacheSize(256*1024*1024)))
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

	/*
		db, err = Open("test.db", writer.NewOpt(), cache)
		if err != nil {
			panic(err)
		}
	*/

	/*
		db.Close()

		files, err := filepath.Glob(fmt.Sprint(db.directory, "/*.lsm"))
		if err != nil {
			panic(err)
		}
		r, err := merge.NewReader(files, cache)
		if err != nil {
			panic(err)
		}
	*/

	tm = time.Now()
	c := E(db.Cursor())
	defer c.Close()

	kv := shared.KV{}
	i := uint32(0)
	more := E(c.First(&kv))
	for more {
		k := binary.BigEndian.Uint32(kv.Key)
		v := binary.BigEndian.Uint32(kv.Value)
		if i != k || i != v {
			log.Panicln("i", i, "k", k, "v", v)
		}
		more = E(c.Next(&kv))
		i++
	}
	fmt.Println("iterated", i, "in", time.Since(tm))

	kv.Key = binary.BigEndian.AppendUint32(nil, uint32(25_578))
	if E(c.Find(&kv)) == reader.Found {
		fmt.Println("found", binary.BigEndian.Uint32(kv.Value))

		if E(c.Next(&kv)) {
			fmt.Println("next", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			if E(c.Next(&kv)) {
				fmt.Println("next", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			}
		}
	}

	kv.Key = binary.BigEndian.AppendUint32(nil, uint32(25_578))
	if E(c.Find(&kv)) == reader.Found {
		fmt.Println("found", binary.BigEndian.Uint32(kv.Value))

		if E(c.Previous(&kv)) {
			fmt.Println("prev", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			if E(c.Previous(&kv)) {
				fmt.Println("prev", binary.BigEndian.Uint32(kv.Key), binary.BigEndian.Uint32(kv.Value))
			}
		}
	}

	/*
		tm = time.Now()
		c, err := db.Cursor()
		if err != nil {
			panic(err)
		}

		kv := shared.KV{}
		i := uint32(0)
		more := c.First(&kv)
		for more {
			k := binary.BigEndian.Uint32(kv.Key)
			v := binary.BigEndian.Uint32(kv.Value)
			if v != i || k != i {
				log.Panicln("i", i, "!=", k, "or", v)
			}
			more = c.Next(&kv)
			i++
		}
		fmt.Println("iterated", i, "in", time.Since(tm))

		db.Close()
	*/
	//fmt.Println("waiting to close")
}
