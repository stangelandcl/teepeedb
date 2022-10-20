package merge

import (
	"encoding/binary"
	"log"
	"os"
	"sort"

	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/shared"
	"github.com/stangelandcl/teepeedb/writer"
)

type merger struct {
	r       *Reader
	w       *writer.File
	delete  bool
	files   []string
	dstfile string
}

// files in order newest to oldest
// hardDelete means remove from file instead of inserting a delete tombstone
// fixedValueSize < 0 == variable size
func newMerger(dstfile string, files []string, cache reader.Cache, hardDelete bool, opts ...writer.Opt) (merger, error) {
	w := merger{
		files:   files,
		dstfile: dstfile,
	}
	r, err := NewReader(files, cache)
	if err != nil {
		return w, err
	}
	f, err := writer.NewFile(dstfile+".tmp", opts...)
	if err != nil {
		r.Close()
		return w, err
	}

	w.r = r
	w.w = f
	w.delete = hardDelete
	return w, nil
}

func (w *merger) Run() error {
	c, err := w.r.Cursor()
	if err != nil {
		return err
	}
	defer c.Close()
	keys := []uint32{}
	kv := shared.KV{}
	more, err := c.First(&kv)
	if err != nil {
		return err
	}
	for more {
		keys = append(keys, binary.BigEndian.Uint32(kv.Key))

		if !kv.Delete || !w.delete {
			err := w.w.Add(&kv)
			if err != nil {
				return err
			}
		}
		more, err = c.Next(&kv)
		if err != nil {
			return err
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	err = w.w.Close()
	if err != nil {
		return err
	}
	w.w = nil // success
	return nil
}

func (w *merger) Close() {
	w.r.Close()

	// if failure
	if w.w != nil {
		w.w.Close()
		os.Remove(w.dstfile + ".tmp")
		return
	}

	// success

	err := os.Rename(w.dstfile+".tmp", w.dstfile)
	if err != nil {
		os.Remove(w.dstfile + ".tmp")
		log.Println("merge failed", w.dstfile, err)
		return
	}

	// remove in reverse order so LSM tree is never in an invalid state
	// removing newest first would leave older entries as the top level values
	// before they get deletes and the new file becomes the top level
	for i := len(w.files) - 1; i >= 0; i-- {
		if w.files[i] != w.dstfile {
			os.Remove(w.files[i])
		}
	}
}

func Merge(dstfile string, files []string, cache reader.Cache, hardDelete bool, opts ...writer.Opt) error {
	if len(files) == 0 {
		return nil
	}
	if len(files) == 1 {
		return os.Rename(files[0], dstfile)
	}
	w, err := newMerger(dstfile, files, cache, hardDelete, opts...)
	if err != nil {
		os.Remove(dstfile + ".tmp")
		return err
	}
	defer w.Close()
	return w.Run()
}
