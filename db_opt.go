package teepeedb

import (
	"time"

	"github.com/stangelandcl/teepeedb/internal/reader"
)

type Opt func(db *DB)

// atomic LRU cache, like 2Q algorithm
type Cache interface {
	reader.Cache
}

// new 2Q cache of blockCount blocks.
// multiply blockCount * block size to estimate cache memory use
func NewCache(blockCount int) Cache {
	return reader.NewCache(blockCount)
}

// cache for holding uncompressed blocks
// only useful when reading compressed data
func WithCache(cache Cache) Opt {
	return func(db *DB) {
		db.cache = cache
	}
}

// cache size in bytes
// only useful when reading compressed data
func WithCacheSize(size int) Opt {
	return func(db *DB) {
		db.cache = NewCache(size / db.blockSize)
	}
}

// size of an individual block in a file. 4K, 8K, 16K, etc. each
// block with contain a binary search of keys and values and can be compressed
// 4K is the default.
func WithBlockSize(size int) Opt {
	return func(db *DB) {
		db.blockSize = size
	}
}

// set merge loop background check frequency
// this is only used as a fallback. every write signals the merger to wakeup
// and move at least level 0 files into level 1, possibly more if level 1
// is then full. the background merge is just used in case a merge gets skipped
// or aborted early to try to cleanup. checks if a merge is needed and only
// performs it if there is level 0 data or a level is over-sized
func WithMergeFrequency(loop time.Duration) Opt {
	return func(db *DB) {
		db.mergeFrequency = loop
	}
}
