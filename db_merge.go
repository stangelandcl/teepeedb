package teepeedb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/stangelandcl/teepeedb/internal/merge"
)

// true if a level at min or greater has data that needs merged
// this is for checking if deletes should be tombstones or real deletes
// lowest level can use real deletes
func (db *DB) hasLowerLevel(min int) bool {
	for i := min; i < 10; i++ {
		_, err := os.Stat(fmt.Sprint(db.directory, "/", "l", i, ".lsm"))
		if err == nil {
			return true
		}
	}
	return false
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
			files, err := filepath.Glob(fmt.Sprint(db.directory, "/", "l0.*.lsm"))
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

			l1 := fmt.Sprint(db.directory, "/l1.lsm")
			_, err = os.Stat(l1)
			if err == nil {
				files = append(files, l1)
			}

			delete := !db.hasLowerLevel(2)

			// merge level 0 into level 1
			err = db.merge(l1, files, delete)
			if err != nil {
				log.Println("error merging into", db.directory, "into l1:", err)
				break
			}

			// merge level 1+ into next lowest level if possible
			db.mergeLowerLevels()

			files, _ = filepath.Glob(fmt.Sprint(db.directory, "/", "*.tmp"))
			for _, f := range files {
				if !strings.Contains(f, "/l0.") {
					fmt.Println("ll still have tmp", f)
				}
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

func (db *DB) mergeLowerLevels() {
	max := db.baseSize
	for i := 1; i < 10; i++ {
		new := fmt.Sprint(db.directory, "/", "l", i, ".lsm")
		fs, err := os.Stat(new)
		max *= db.multiplier
		if err != nil || fs.Size() < int64(max) {
			continue
		}

		old := fmt.Sprint(db.directory, "/", "l", i+1, ".lsm")
		delete := !db.hasLowerLevel(i + 2)

		files := []string{new}
		_, err = os.Stat(old)
		if err == nil {
			files = append(files, old)
		}
		err = db.merge(old, files, delete)
		if err != nil {
			log.Println("error merging into", db.directory, "into l1:", err)
			break
		}
	}
}

func (db *DB) merge(dstfile string, files []string, delete bool) error {
	m, err := merge.NewMerger(dstfile, files, db.cache, delete, db.blockSize, db.valueSize, db.compression)
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
