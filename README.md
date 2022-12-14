# TeepeeDB

Simple Log-Structured Merge tree in go

Useful as a batch database for fast reads

Only allows sorted batch inserts. Individual writes should be queued and sorted before attempting insert.

Naive merging. Does not split files for faster merging. Instead merges whole files into each level.

Uses LZ4 compression. Handles 100 million keys with smallish values with no problems as long as inserts aren't in too small a batches or too constant.

Merges happen in background goroutines. No prefix key compression, but LZ4 should accomplish the same thing.
Intended for one LSM DB per table/dataset and no transactions across tables.
same process is not an issue. No cache so query in bulk in sorted order

Uses memory mapping for reads.

A teepee has the same basic shape as a log-structured merge tree, triangular. More importantly teepeedb is fun to say.

### Example
From db_test.go TestExample()

```go
db, err := Open("test.db")
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
tmp := [4]byte{}
for i := 0; i < count; i++ {
    // data is sorted in memcmp/bytes.Compare order so use big-endian
    // when serializing integers as keys to maintain their numeric order
    binary.BigEndian.PutUint32(tmp[:], uint32(i))

    // inserts and deletes must happen in sorted order within a transaction
    // .Add() will fail if bytes.Compare(k, lastKey) <= 0
    err = w.Add(tmp[:], tmp[:])
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
for j := 0; j < 3; j++ {
    i = 0
    tm = time.Now()
    // always call first/last/find before next/previous
    more := c.First()
    for more {
        kv := c.Current()
        k := binary.BigEndian.Uint32(kv.Key)
        v := binary.BigEndian.Uint32(kv.Value)
        if i != k || i != v {
            log.Panicln("i", i, "k", k, "v", v)
        }
        more = c.Next()
        i++
    }
    fmt.Println("iterated", i, "in", time.Since(tm))

    tm = time.Now()
    i = uint32(0)
    // always call first/last/find before next/previous
    more = c.First()
    for more {
        k := binary.BigEndian.Uint32(c.Key())
        if i != k {
            log.Panicln("i", i, "k", k)
        }
        more = c.Next()
        i++
    }
    fmt.Println("iterated keys", i, "in", time.Since(tm))
}

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
    found := c.Find(buf[:])
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
```
