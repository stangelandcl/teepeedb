package teepeedb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/stangelandcl/teepeedb/internal/merge"
	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/writer"
)

type DB struct {
	directory string
	// one write at a time
	writeLock sync.Mutex
	// so opening a cursor and closing the reader don't overlap
	readLock sync.Mutex
	// so deleting old files from merge doesn't coincide with opening
	// a new reader on those files
	mergeLock  sync.Mutex
	counter    int64
	mergerChan chan int
	wg         sync.WaitGroup
	reader     *merge.Reader
	done       bool

	// options
	compression shared.Compression
	blockSize   int
	valueSize   int
	cache       reader.Cache
}

type Stats struct {
	// number of data blocks
	DataBlocks int
	// total size of compressed data block bytes
	DataBytes int
	// number of deletes
	Deletes int
	// number of index blocks
	IndexBlocks int
	// total size of compressed index block bytes
	IndexBytes int
	// number of inserts
	Inserts int
}

// estimated compressed size
func (s Stats) Size() int {
	return s.DataBytes + s.IndexBytes
}

// estimated counts: inserts - deletes >= 0
func (s Stats) Count() int {
	count := s.Inserts - s.Deletes
	if count < 0 {
		count = 0
	}
	return count
}

func Open(directory string, opts ...Opt) (*DB, error) {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}
	db := &DB{
		directory:  directory,
		mergerChan: make(chan int, 2),

		/* options */
		cache:       &reader.NullCache{},
		blockSize:   4096,
		valueSize:   -1,
		compression: shared.Raw,
	}
	err = db.resetReader()
	if err != nil {
		close(db.mergerChan)
		return nil, err
	}

	db.wg.Add(1)
	go db.mergeLoop()

	return db, nil
}

func (db *DB) Stats() Stats {
	db.readLock.Lock()
	defer db.readLock.Unlock()

	st := db.reader.Stats()
	rs := Stats{}
	for _, s := range st.Footers {
		rs.DataBlocks += s.DataBlocks
		rs.DataBytes += s.DataBytes
		rs.Deletes += s.Deletes
		rs.IndexBlocks += s.IndexBlocks
		rs.IndexBytes += s.IndexBytes
		rs.Inserts += s.Inserts
	}
	return rs
}

func (db *DB) resetReader() error {
	var r *merge.Reader
	err := func() error {
		// lock so merger can't delete files while we are opening them
		db.mergeLock.Lock()
		defer db.mergeLock.Unlock()

		matches, err := filepath.Glob(fmt.Sprintf("%v/*.lsm", db.directory))
		if err != nil {
			return err
		}

		r, err = merge.NewReader(matches, db.cache)
		return err
	}()
	if err != nil {
		return err
	}

	db.readLock.Lock()
	defer db.readLock.Unlock()
	old := db.reader
	// atomic so
	db.reader = r
	if old != nil {
		old.Close()
	}

	return nil
}

func (db *DB) Cursor() (Cursor, error) {
	db.readLock.Lock()
	defer db.readLock.Unlock()

	c := Cursor{}
	var err error
	c.m, err = db.reader.Cursor()
	return c, err
}

func (db *DB) Write() (Writer, error) {
	db.writeLock.Lock()
	if db.done {
		db.writeLock.Unlock()
		return Writer{}, fmt.Errorf("teepeedb: database closed")
	}

	c := atomic.AddInt64(&db.counter, 1) // TODO: reset counter to zero after merge if empty
	filename := fmt.Sprintf("%v/l0.%016d.lsm", db.directory, c)
	w, err := writer.NewFile(filename+".tmp", db.blockSize, db.valueSize, db.compression)
	if err != nil {
		db.writeLock.Unlock()

		return Writer{}, err
	}

	return Writer{
		db:       db,
		filename: filename,
		w:        w,
	}, nil
}

func (db *DB) Close() {
	// allow double close
	if db.reader == nil && db.mergerChan == nil {
		return
	}
	db.done = true

	if !db.writeLock.TryLock() {
		log.Println("teepeedb: waiting for write to close")
		db.writeLock.Lock()
	}
	defer db.writeLock.Unlock()

	close(db.mergerChan)
	//fmt.Println("waiting to close for merger")
	db.wg.Wait()
	db.mergerChan = nil
	//fmt.Println("closing")
	db.reader.Close()
	db.reader = nil
}
