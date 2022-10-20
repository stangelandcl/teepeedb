package teepeedb

import (
	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/shared"
)

type Opt func(db *DB)

type Cache interface {
	reader.Cache
}

func NewCache(size int) Cache {
	return reader.NewCache(size)
}

func WithCache(cache Cache) Opt {
	return func(db *DB) {
		db.cache = cache
	}
}

func WithCacheSize(size int) Opt {
	return func(db *DB) {
		db.cache = NewCache(size)
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

// size of value if fixed size, 0 if no value just keys, -1 for variable size value
// -1 is the default
func WithValueSize(size int) Opt {
	return func(db *DB) {
		db.valueSize = size
	}
}

// use LZ4 to compress each block.
// decreases size but could increase random read performance
func WithLz4() Opt {
	return func(db *DB) {
		db.compression = shared.Lz4
	}
}
