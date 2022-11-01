package merge

import (
	"bytes"

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type Cursor struct {
	reader  *Reader
	cursors []*reader.Cursor
	heap    heap
	closed  bool
	key     []byte
}

func (c *Cursor) Close() {
	if !c.closed {
		// decrement refcount and possibly close reader
		c.reader.Close()
		c.closed = true
	}
}

func (c *Cursor) First() (more, delete bool) {
	return c.end(1)
}

func (c *Cursor) Last() (more, delete bool) {
	return c.end(-1)
}

func (c *Cursor) end(order int) (more, delete bool) {
	c.heap.Values = c.heap.Values[:0]
	for i, cur := range c.cursors {
		key := Position{
			Cursor: cur,
			Index:  i,
		}

		var found bool
		if order == 1 {
			found = key.Cursor.First()
		} else {
			found = key.Cursor.Last()
		}
		if found {
			key.Key, key.Delete = key.Cursor.Key()
			c.heap.Values = append(c.heap.Values, key)
		}
	}
	if len(c.heap.Values) == 0 {
		return false, false
	}
	c.heap.Init(order)
	key := &c.heap.Values[0]
	return true, key.Delete
}

func (c *Cursor) Next() (more, delete bool) {
	return c.move(1)
}

func (c *Cursor) Previous() (more, delete bool) {
	return c.move(-1)
}

func (c *Cursor) move(order int) (more, delete bool) {
	next := order == 1
	c.key = append(c.key[:0], c.heap.Values[0].Key...)
	key := &c.heap.Values[0]
	// increment cursor for current key and for all older levels
	// that are equal to that key
	for {
		idx := key.Index
		var found bool
		if next {
			found = key.Cursor.Next()
		} else {
			found = key.Cursor.Previous()
		}
		if found {
			key.Key, key.Delete = key.Cursor.Key()
			c.heap.Fix(0)
			// same cursor is the lowest key so we are done
			if c.heap.Values[0].Index == idx {
				break
			}
		} else {
			// this cursor is at its iteration endpoint
			c.heap.Pop()
			if len(c.heap.Values) == 0 {
				return false, false
			}
		}
		key := &c.heap.Values[0]
		if bytes.Compare(key.Key, c.key)*order > 0 {
			break
		}
	}

	return true, key.Delete
}

/*
func (c *Cursor) Get(key []byte) (found, delete bool) {
	c.heap.Values = c.heap.Values[:0]
	for _, cur := range c.cursors {
		found := cur.Find(key)
		if found == reader.Found {
			_, delete = cur.Key()
			return true, delete
		}
	}
	return false, false
}
*/

// returns Found for exact match
// Partial for found a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(find []byte) (reader.FindResult, bool) {
	c.heap.Values = c.heap.Values[:0]
	for i, cur := range c.cursors {
		key := Position{
			Cursor: cur,
			Index:  i,
		}
		found := key.Cursor.Find(find)
		if found > 0 {
			key.Key, key.Delete = key.Cursor.Key()
			c.heap.Values = append(c.heap.Values, key)
		}
	}

	c.heap.Init(1)
	if len(c.heap.Values) == 0 {
		return reader.NotFound, false
	}

	v := &c.heap.Values[0]
	found := bytes.Equal(v.Key, find)
	if found {
		return reader.Found, v.Delete
	}
	return reader.FoundGreater, v.Delete
}

func (c *Cursor) Current(which block.Which, kv *shared.KV) {
	c.heap.Values[0].Cursor.Current(which, kv)
}
