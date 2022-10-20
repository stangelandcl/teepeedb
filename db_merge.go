package teepeedb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/stangelandcl/teepeedb/merge"
)

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
		case <-time.After(60 * time.Second):
		}

		//fmt.Println("merger awake")
		for {
			files, err := filepath.Glob(fmt.Sprint(db.directory, "/", "l0.*.lsm"))
			if err != nil {
				log.Println("error globbing l0 files in", db.directory, err)
				break
			}
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

			//log.Println("merging", len(files), "files in", db.directory)
			delete := !db.hasLowerLevel(2)

			//log.Println("merging", files, "->", l1)
			//tm := time.Now()
			//fmt.Println("merging", len(files), "files ->", l1)
			err = merge.Merge(l1, files, db.cache, delete, db.blockSize, db.valueSize, db.compression)
			if err != nil {
				log.Println("error merging into", db.directory, "into l1:", err)
				break
			}
			//log.Println("merged", len(files), "in", db.directory, "in", time.Since(tm))

			files, _ = filepath.Glob(fmt.Sprint(db.directory, "/", "*.tmp"))
			for _, f := range files {
				if !strings.Contains(f, "/l0.") {
					fmt.Println("still have tmp", f)
				}
			}

			db.mergeLowerLevels()

			files, _ = filepath.Glob(fmt.Sprint(db.directory, "/", "*.tmp"))
			for _, f := range files {
				if !strings.Contains(f, "/l0.") {
					fmt.Println("ll still have tmp", f)
				}
			}
			err = db.resetReader()
			if err != nil {
				log.Println("error reopening readers", db.directory, err)
				break
			}
		}
	}

	files, _ := filepath.Glob(fmt.Sprint(db.directory, "/", "*.tmp"))
	for _, f := range files {
		//fmt.Println("removing tmp", f)
		os.Remove(f)
	}

	//fmt.Println("merging done")
	db.wg.Done()
}

func (db *DB) mergeLowerLevels() {
	baseSize := 16 * 1024 * 1024
	multiplier := 10
	for i := 1; i < 10; i++ {
		new := fmt.Sprint(db.directory, "/", "l", i, ".lsm")
		fs, err := os.Stat(new)
		max := baseSize
		baseSize *= multiplier
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
		//tm := time.Now()
		//fmt.Println("merging level", i, "into", i+1)
		err = merge.Merge(old, files, db.cache, delete, db.blockSize, db.valueSize, db.compression)
		if err != nil {
			log.Println("error merging into", db.directory, "into l1:", err)
			break
		}
		//fmt.Println("merged", db.directory, "level", i, "into", i+1, "in", time.Since(tm))
	}
}

func (db *DB) wakeMerger() {
	select {
	case db.mergerChan <- 1:
	default:
	}
}
