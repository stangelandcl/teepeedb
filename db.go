package teepeedb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/stangelandcl/teepeedb/internal/merge"
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
	mergeLock sync.Mutex
	// counter counts down so lower numbered L0 files are newer values
	// and can be sorted the same as L1,L2,L3 files etc which are the same
	// lower numbered files contain newer values
	counter         int64
	mergerChan      chan int
	mergerWaitGroup sync.WaitGroup
	reader          *merge.Reader
	closed          bool

	// options
	blockSize      int
	mergeFrequency time.Duration

	// size of level 1
	baseSize int
	// size of level N + 1 = multiplier + size of level N
	multiplier int
}

const (
	counterMax = 999_999_999_999_999
	maxLevel   = 10
)

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
	// uncompressed key bytes - deletes and inserts
	KeyBytes int
	// uncompressed value bytes
	ValueBytes int
}

// estimated compressed size
func (s Stats) CompressedSize() int {
	return s.DataBytes + s.IndexBytes
}

// estimated uncompressed size
func (s Stats) UncompressedSize() int {
	return s.ValueBytes + s.KeyBytes
}

// estimated counts: inserts - deletes >= 0
func (s Stats) Count() int {
	count := s.Inserts - s.Deletes
	if count < 0 {
		count = 0
	}
	return count
}

// create or open database in directory. directory will be created
// if it doesn't exist
func Open(directory string, opts ...Opt) (*DB, error) {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}
	db := &DB{
		directory:  directory,
		mergerChan: make(chan int, 2),
		counter:    counterMax,

		// options
		blockSize:      4096,
		mergeFrequency: time.Hour,

		baseSize:   16 * 1024 * 1024,
		multiplier: 10,
	}
	for _, opt := range opts {
		opt(db)
	}

	err = db.reloadReader()
	if err != nil {
		close(db.mergerChan)
		return nil, err
	}

	db.mergerWaitGroup.Add(1)
	go db.mergeLoop()

	return db, nil
}

// return summed stats from each underlying level to estimate
// size and counts quickly. stats are stored in each file not calculated
// on the fly for speed
func (db *DB) Stats() Stats {
	db.readLock.Lock()
	defer db.readLock.Unlock()

	st := db.reader.Stats()
	rs := Stats{}
	for _, s := range st.Footers {
		rs.DataBlocks += s.DataBlocks
		rs.DataBytes += s.CompressedDataBytes
		rs.Deletes += s.Deletes
		rs.IndexBlocks += s.IndexBlocks
		rs.IndexBytes += s.CompressedIndexBytes
		rs.Inserts += s.Inserts
		rs.KeyBytes = s.RawKeyBytes
		rs.ValueBytes = s.RawValueBytes
	}
	return rs
}

// close old reader and open new
// atomic with respect to the final rename and cleanup of merged files
// also with respect to opening a cursor
func (db *DB) reloadReader() error {
	var r *merge.Reader
	err := func() error {
		// lock so merger can't delete files while we are opening them
		db.mergeLock.Lock()
		defer db.mergeLock.Unlock()

		matches, err := filepath.Glob(fmt.Sprintf("%v/*.lsm", db.directory))
		if err != nil {
			return err
		}

		sort.Slice(matches, func(i, j int) bool {
			return matches[i] < matches[j]
		})
		r, err = merge.NewReader(matches)
		return err
	}()
	if err != nil {
		return err
	}

	// atomic with Cursor() so new cursors cannot be opened while a
	// reader is being closed
	db.readLock.Lock()
	defer db.readLock.Unlock()

	old := db.reader
	db.reader = r
	if old != nil {
		old.Close()
	}

	return nil
}

func (db *DB) Cursor() Cursor {
	db.readLock.Lock()
	defer db.readLock.Unlock()

	c := Cursor{}
	c.m = db.reader.Cursor()
	return c
}

func (db *DB) Write() (Writer, error) {
	db.writeLock.Lock()
	if db.closed {
		db.writeLock.Unlock()
		return Writer{}, fmt.Errorf("teepeedb: database closed")
	}

	files, err := filepath.Glob(fmt.Sprintf("%v/l00.*.lsm", db.directory))
	if err == nil && len(files) == 0 {
		db.counter = counterMax
	}

	filename := fmt.Sprintf("%v/l00.%015d.lsm", db.directory, db.counter)
	db.counter--
	w, err := writer.NewFile(filename+".tmp", db.blockSize)
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

// caller's responsibility to ensure no more new reads or writes come in once
// close has started.
func (db *DB) Close() {
	// allow double close
	if db.mergerChan == nil {
		return
	}
	db.closed = true

	if !db.writeLock.TryLock() {
		log.Println("teepeedb: waiting for write to close")
		db.writeLock.Lock()
	}
	defer db.writeLock.Unlock()

	// signal merge to close
	close(db.mergerChan)
	// wait for merger to close
	db.mergerWaitGroup.Wait()
	db.mergerChan = nil

	db.reader.Close()
	db.reader = nil
}
