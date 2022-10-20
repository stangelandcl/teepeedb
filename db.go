package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/stangelandcl/teepeedb/merge"
	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/writer"
)

type DB struct {
	directory string
	// one write at a time
	writeLock sync.Mutex
	// so opening a cursor and closing the reader don't overlap
	readLock   sync.Mutex
	counter    int64
	opt        writer.Opt
	mergerChan chan int
	cache      reader.Cache
	wg         sync.WaitGroup
	reader     *merge.Reader
	done       bool
}

type Cursor struct {
	merge.Cursor
}

func Open(directory string, opts ...Opt) (*DB, error) {
	opt := NewOpt()
	if len(opts) > 0 {
		opt = opts[0]
	}
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}
	db := &DB{
		directory:  directory,
		opt:        opt.w,
		mergerChan: make(chan int, 2),
		cache:      opt.cache,
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

func (db *DB) resetReader() error {
	matches, err := filepath.Glob(fmt.Sprintf("%v/*.lsm", db.directory))
	if err != nil {
		return err
	}

	r, err := merge.NewReader(matches, db.cache)
	if err != nil {
		return err
	}

	func() {
		db.readLock.Lock()
		defer db.readLock.Unlock()
		old := db.reader
		// atomic so
		db.reader = r
		if old != nil {
			old.Close()
		}
	}()
	return nil
}

func (db *DB) Cursor() (Cursor, error) {
	db.readLock.Lock()
	defer db.readLock.Unlock()

	c := Cursor{}
	var err error
	c.Cursor, err = db.reader.Cursor()
	return c, err
}

func (db *DB) Write() (Writer, error) {
	db.writeLock.Lock()
	if db.done {
		db.writeLock.Unlock()
		return Writer{}, fmt.Errorf("database closed")
	}

	c := atomic.AddInt64(&db.counter, 1) // TODO: reset counter to zero after merge if empty
	filename := fmt.Sprintf("%v/l0.%016d.lsm", db.directory, c)
	w, err := writer.NewFile(filename+".tmp", db.opt)
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
	db.done = true

	if !db.writeLock.TryLock() {
		fmt.Println("waiting for write to close")
		db.writeLock.Lock()
	}
	defer db.writeLock.Unlock()

	close(db.mergerChan)
	//fmt.Println("waiting to close for merger")
	db.wg.Wait()
	//fmt.Println("closing")
	db.reader.Close()
	db.reader = nil
}
