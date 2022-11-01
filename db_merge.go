package teepeedb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/stangelandcl/teepeedb/internal/merge"
)

// true if a level at min or greater has data that needs merged
// this is for checking if deletes should be tombstones or real deletes
// lowest level can use real deletes
func (db *DB) hasLowerLevel(min int) bool {
	for i := min; i < maxLevel; i++ {
		_, err := os.Stat(fmt.Sprintf("%v/%02d.lsm", db.directory, i))
		if err == nil {
			return true
		}
	}
	return false
}

func fileSize(files ...string) int {
	sz := 0
	for _, file := range files {
		x, err := os.Stat(file)
		if err != nil {
			continue
		}
		sz += int(x.Size())
	}
	return sz
}

func (db *DB) mergeLoop() {
	alive := true
	for alive {
		select {
		case _, alive = <-db.mergerChan:
		case <-time.After(db.mergeFrequency):
		}

		// loop because maybe new data came in as we were merging
		for {
			files, err := filepath.Glob(fmt.Sprintf("%v/l00.*.lsm", db.directory))
			if err != nil {
				log.Println("error globbing l0 files in", db.directory, err)
				break
			}
			// continue merging until there is no more new data to push down
			// the tree
			if len(files) == 0 {
				break
			}
			sort.Slice(files, func(i, j int) bool {
				return files[i] < files[j]
			})

			totalSize := fileSize(files...)

			max := db.baseSize

			var dst string
			i := 1
			for ; i < maxLevel; i++ {
				current := max
				max *= db.multiplier
				dst = fmt.Sprintf("%v/l%02d.lsm", db.directory, i)
				_, err = os.Stat(dst)
				if err == nil {
					files = append(files, dst)
					totalSize += fileSize(dst)
				}

				if totalSize < current {
					break
				}
			}

			delete := !db.hasLowerLevel(i + 1)

			// merge level 0 into level i
			err = db.merge(dst, files, delete)
			if err != nil {
				log.Println("error merging into", db.directory, "into l1:", err)
				break
			}

			err = db.reloadReader()
			if err != nil {
				log.Println("error reopening readers", db.directory, err)
				break
			}
		}
	}

	files, _ := filepath.Glob(fmt.Sprint(db.directory, "/", "*.tmp"))
	for _, f := range files {
		os.Remove(f)
	}

	db.mergerWaitGroup.Done()
}

func (db *DB) merge(dstfile string, files []string, delete bool) error {
	m, err := merge.NewMerger(dstfile, files, delete, db.blockSize)
	if err != nil {
		return err
	}
	err = m.Run()
	if err != nil {
		m.Close()
		return err
	}
	func() {
		db.mergeLock.Lock()
		defer db.mergeLock.Unlock()

		// lock during file renames so reader opening at the same time
		// isn't trying to open as we are deleting
		err = m.Commit()
	}()
	m.Close()
	return err
}

// non-blocking try-wake merger
func (db *DB) wakeMerger() {
	select {
	case db.mergerChan <- 1:
	default:
	}
}
