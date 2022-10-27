package teepeedb

import (
	"bytes"
	"fmt"
	"os"

	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/writer"
)

type Writer struct {
	db                *DB
	filename          string
	w                 *writer.File
	last              []byte
	closed, committed bool
}

// inserts and deletes must happen in sorted order within a transaction
// fails if bytes.Compare(k, lastKey) <= 0
func (w *Writer) Add(key, val []byte) error {
	if bytes.Compare(w.last, key) >= 0 {
		return fmt.Errorf("teepeedb: adding keys out of order. last: %v current: %v", w.last, key)
	}
	w.last = append(w.last[:0], key...)
	kv := shared.KV{}
	kv.Key = key
	kv.Value = val
	return w.w.Add(&kv)
}

// inserts and deletes must happen in sorted order within a transaction
// fails if bytes.Compare(k, lastKey) <= 0
func (w *Writer) Delete(key []byte) error {
	if bytes.Compare(w.last, key) >= 0 {
		return fmt.Errorf("teepeedb: adding keys out of order. last: %v current: %v", w.last, key)
	}
	w.last = append(w.last[:0], key...)
	kv := shared.KV{}
	kv.Key = key
	kv.Delete = true
	return w.w.Add(&kv)
}

// commit transaction to disk.
// writes happen to temp file.
// this syncs temp file to disk, renames file to make it a part of LSM tree
// re-opens readers so next Cursor() call sees new data and triggers
// background merger to wakeup and merge this level 0 file into level 1
func (w *Writer) Commit() error {
	err := w.w.Commit()
	if err != nil {
		return err
	}

	// no writes to this file. we're done
	if len(w.last) == 0 {
		os.Remove(w.filename + ".tmp")
		w.committed = true
		return nil
	}

	// commit
	err = os.Rename(w.filename+".tmp", w.filename)
	if err == nil {
		w.committed = true
		// so next open cursor sees changes
		err = w.db.reloadReader()

		// make sure file gets merged into level 1 as soon as possible.
		// there can be multiple level 0 files but only of each other level
		w.db.wakeMerger()
	}
	return err
}

func (w *Writer) Close() {
	if w.closed {
		return
	}
	w.db.writeLock.Unlock()
	w.closed = true
	w.w.Close()
	if !w.committed {
		os.Remove(w.filename + ".tmp")
	}
}
