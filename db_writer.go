package teepeedb

import (
	"bytes"
	"fmt"
	"os"

	"github.com/stangelandcl/teepeedb/internal/shared"
	"github.com/stangelandcl/teepeedb/internal/writer"
)

type Writer struct {
	db       *DB
	filename string
	w        *writer.File
	last     []byte
	closed   bool
}

// add in sorted order only
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

// add in sorted order only
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

func (w *Writer) Commit() error {
	err := w.w.Close()
	if err != nil {
		return err
	}

	// no writes to this file
	if len(w.last) == 0 {
		os.Remove(w.filename + ".tmp")
		return nil
	}

	// commit
	err = os.Rename(w.filename+".tmp", w.filename)
	if err == nil {
		w.w = nil                // so close knows it committed
		err = w.db.resetReader() // so next open cursor sees changes
		w.db.wakeMerger()
	}
	return err
}

func (w *Writer) Close() {
	if w.closed {
		return
	}
	if w.w != nil { // not committed
		w.w.Close()
		os.Remove(w.filename + ".tmp")
	}
	w.db.writeLock.Unlock()
	w.closed = true
}
