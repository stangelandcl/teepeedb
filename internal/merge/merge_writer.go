package merge

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/writer"
)

type merger struct {
	r         *Reader
	w         *writer.File
	delete    bool
	files     []string
	dstfile   string
	committed bool
}

// files in order newest to oldest
// hardDelete means remove from file instead of inserting a delete tombstone
// fixedValueSize < 0 == variable size
func NewMerger(
	dstfile string,
	files []string,
	cache reader.Cache,
	hardDelete bool,
	blockSize, valueSize int,
	compression shared.BlockFormat) (merger, error) {
	if len(files) == 0 {
		return merger{}, fmt.Errorf("teepeedb: no files to merge")
	}
	w := merger{
		files:   files,
		dstfile: dstfile,
	}
	var err error
	if len(files) > 1 {
		w.r, err = NewReader(files, cache)
		if err != nil {
			return w, err
		}
		w.w, err = writer.NewFile(dstfile+".tmp", blockSize, valueSize, compression)
		if err != nil {
			w.r.Close()
			return w, err
		}
	}

	w.delete = hardDelete
	return w, nil
}

func (w *merger) Run() error {
	if len(w.files) == 1 {
		return nil
	}
	c := w.r.Cursor()
	defer c.Close()
	keys := []uint32{}
	kv := shared.KV{}
	more := c.First(&kv)
	for more {
		keys = append(keys, binary.BigEndian.Uint32(kv.Key))

		if !kv.Delete || !w.delete {
			err := w.w.Add(&kv)
			if err != nil {
				return err
			}
		}
		more = c.Next(&kv)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	err := w.w.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (w *merger) Commit() error {
	var err error
	if len(w.files) == 1 {
		err = os.Rename(w.files[0], w.dstfile)
	} else {
		err = os.Rename(w.dstfile+".tmp", w.dstfile)
	}
	if err != nil {
		os.Remove(w.dstfile + ".tmp")
		log.Println("merge failed", w.dstfile, err)
		return err
	}
	w.committed = true

	// remove in reverse order so LSM tree is never in an invalid state
	// removing newest first would leave older entries as the top level values
	// before they get deletes and the new file becomes the top level
	for i := len(w.files) - 1; i >= 0; i-- {
		if w.files[i] != w.dstfile {
			os.Remove(w.files[i])
		}
	}
	return nil
}

func (m *merger) Close() {
	if m.r != nil {
		m.r.Close()
		m.r = nil
	}

	// happens when moving a single file instead of merging
	if m.w != nil {
		m.w.Close()
	}
	if !m.committed {
		os.Remove(m.dstfile + ".tmp")
		m.committed = true
	}
}
