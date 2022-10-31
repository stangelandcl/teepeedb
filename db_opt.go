package teepeedb

import (
	"time"
)

type Opt func(db *DB)

// size of an individual block in a file. 1K, 2K, 4K, 8K, 16K, 32K
// block with contain a binary search of keys and values and can be compressed
// 4K is the default.
// 32768 is max size since blocks store 16 bit offsets internally
// and use 1 bit as a delete flag
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
// 1 hour is the default
func WithMergeFrequency(loop time.Duration) Opt {
	return func(db *DB) {
		db.mergeFrequency = loop
	}
}
