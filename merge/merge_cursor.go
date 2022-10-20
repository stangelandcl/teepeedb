package merge

import (
	"bytes"

	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/shared"
)

type Cursor struct {
	reader  *Reader
	cursors []*reader.Cursor
	heap    heap
	closed  bool
}

func (c *Cursor) Close() {
	c.reader.mutex.Lock()
	defer c.reader.mutex.Unlock()

	if !c.closed {
		c.reader.cursors--
		c.closed = true
	}
}

func (c *Cursor) First(kv *shared.KV) bool {
	return c.end(kv, 1, reader.First)
}

func (c *Cursor) Last(kv *shared.KV) bool {
	return c.end(kv, -1, reader.Last)
}

func (c *Cursor) end(kv *shared.KV, order int, start reader.Move) bool {
	c.heap.Values = nil
	for i, cur := range c.cursors {
		key := Position{
			Cursor: cur,
			Index:  i,
		}

		if key.Cursor.Move(start, &key.KV) {
			c.heap.Values = append(c.heap.Values, key)
		}
	}
	c.heap.Init(order)
	if len(c.heap.Values) == 0 {
		return false
	}
	key := &c.heap.Values[0]
	*kv = key.KV
	return true
}

func (c *Cursor) Next(kv *shared.KV) bool {
	return c.move(kv, 1, reader.Next)
}

func (c *Cursor) Previous(kv *shared.KV) bool {
	return c.move(kv, -1, reader.Previous)
}

func (c *Cursor) move(kv *shared.KV, order int, dir reader.Move) bool {
	last := c.heap.Pop()

	// increment cursor for current key and for all older levels
	// that are less or equal to that key
	for len(c.heap.Values) > 0 {
		key := &c.heap.Values[0]
		if bytes.Compare(key.KV.Key, last.KV.Key)*order > 0 {
			break
		}
		if key.Cursor.Move(dir, &key.KV) {
			c.heap.Fix(0)
		} else {
			c.heap.Pop()
		}
	}

	if last.Cursor.Move(dir, &last.KV) {
		c.heap.Push(last)
	}

	if len(c.heap.Values) == 0 {
		return false
	}
	key := &c.heap.Values[0]
	*kv = key.KV
	return true
}

func (c *Cursor) Lookup(kv *shared.KV) bool {
	c.heap.Values = nil
	for _, cur := range c.cursors {
		tmp := *kv
		if cur.Find(&tmp) == reader.Found {
			*kv = tmp
			return true
		}
	}
	return false
}

func (c *Cursor) Find(kv *shared.KV) reader.FindResult {
	c.heap.Values = nil
	for i, cur := range c.cursors {
		key := Position{
			Cursor: cur,
			Index:  i,
		}
		key.KV.Key = kv.Key
		if key.Cursor.Find(&key.KV) > 0 {
			c.heap.Values = append(c.heap.Values, key)
		}
	}

	c.heap.Init(1)
	if len(c.heap.Values) == 0 {
		return reader.NotFound
	}

	v := &c.heap.Values[0]
	found := bytes.Equal(v.KV.Key, kv.Key)
	*kv = v.KV
	if found {
		return reader.Found
	}
	return reader.Partial
}
