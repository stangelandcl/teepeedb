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
		if size < 512 {
			size = 512
		} else if size > 32768 {
			size = 32768
		}
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

// set size in bytes of level 1
// level 0 size is unbounded
// levels 2-9 sizes are a multiple of level 1 size
// default is 16 MB
func WithBaseSize(sz int) Opt {
	return func(db *DB) {
		if sz < 1024 {
			sz = 1024
		}
		db.baseSize = sz
	}
}

// set multiplier to increase baseSize by when moving to higher levels
// for example baseSize = 16MB and multiplier = 10
// L1 = 16 MB, L2 = 160MB, L3 = 1600MB, L4 = 16000 MB, L5 = 160 GB, L6 = 1600 GB, L7 = 16 TB, L8 = 160 TB L9 = 1600 TB
// there are at most 9 levels including level zero so make sure baseSize * mult ^ 9 is far more than needed
// default is 10
func WithMultiplier(mult int) Opt {
	return func(db *DB) {
		if mult < 2 {
			mult = 2
		}
		db.multiplier = mult
	}
}
