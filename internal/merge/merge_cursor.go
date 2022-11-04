package merge

import (
	"bytes"

	"github.com/stangelandcl/teepeedb/internal/reader"
)

type Cursor struct {
	reader  *Reader
	cursors []*reader.Cursor
	heap    heap
	closed  bool
	last    []byte
	Key     []byte
	Delete  bool
}

func (c *Cursor) Close() {
	if !c.closed {
		// decrement refcount and possibly close reader
		c.reader.Close()
		c.closed = true
	}
}

func (c *Cursor) First() bool {
	return c.end(1)
}

func (c *Cursor) Last() bool {
	return c.end(-1)
}

func (c *Cursor) end(order int) (more bool) {
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
		return false
	}
	c.heap.Init(order)
	key := &c.heap.Values[0]
	c.Key = key.Key
	c.Delete = key.Delete
	return true
}

func (c *Cursor) Next() bool {
	return c.move(1)
}

func (c *Cursor) Previous() bool {
	return c.move(-1)
}

func (c *Cursor) move(order int) (more bool) {
	next := order == 1
	c.last = append(c.last[:0], c.heap.Values[0].Key...)
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
				return false
			}
		}
		key := &c.heap.Values[0]
		if bytes.Compare(key.Key, c.last)*order > 0 {
			break
		}
	}

	c.Key = key.Key
	c.Delete = key.Delete
	return true
}

// returns Found for exact match
// Partial for found a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(find []byte) reader.FindResult {
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
		return reader.NotFound
	}

	v := &c.heap.Values[0]
	found := bytes.Equal(v.Key, find)
	rs := reader.FoundGreater
	if found {
		rs = reader.Found
	}
	c.Key = v.Key
	c.Delete = v.Delete
	return rs
}

func (c *Cursor) Value() []byte {
	return c.heap.Values[0].Cursor.Value()
}
