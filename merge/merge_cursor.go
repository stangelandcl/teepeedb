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

func (c *Cursor) First(kv *shared.KV) (bool, error) {
	return c.end(kv, 1, reader.First)
}

func (c *Cursor) Last(kv *shared.KV) (bool, error) {
	return c.end(kv, -1, reader.Last)
}

func (c *Cursor) end(kv *shared.KV, order int, start reader.Move) (bool, error) {
	c.heap.Values = nil
	for i, cur := range c.cursors {
		key := Position{
			Cursor: cur,
			Index:  i,
		}

		found, err := key.Cursor.Move(start, &key.KV)
		if err != nil {
			return false, err
		}
		if found {
			c.heap.Values = append(c.heap.Values, key)
		}
	}
	c.heap.Init(order)
	if len(c.heap.Values) == 0 {
		return false, nil
	}
	key := &c.heap.Values[0]
	*kv = key.KV
	return true, nil
}

func (c *Cursor) Next(kv *shared.KV) (bool, error) {
	return c.move(kv, 1, reader.Next)
}

func (c *Cursor) Previous(kv *shared.KV) (bool, error) {
	return c.move(kv, -1, reader.Previous)
}

func (c *Cursor) move(kv *shared.KV, order int, dir reader.Move) (bool, error) {
	last := c.heap.Pop()

	// increment cursor for current key and for all older levels
	// that are less or equal to that key
	for len(c.heap.Values) > 0 {
		key := &c.heap.Values[0]
		if bytes.Compare(key.KV.Key, last.KV.Key)*order > 0 {
			break
		}
		found, err := key.Cursor.Move(dir, &key.KV)
		if err != nil {
			return false, err
		}
		if found {
			c.heap.Fix(0)
		} else {
			c.heap.Pop()
		}
	}

	found, err := last.Cursor.Move(dir, &last.KV)
	if err != nil {
		return false, err
	}
	if found {
		c.heap.Push(last)
	}

	if len(c.heap.Values) == 0 {
		return false, nil
	}
	key := &c.heap.Values[0]
	*kv = key.KV
	return true, nil
}

func (c *Cursor) Get(kv *shared.KV) (bool, error) {
	c.heap.Values = nil
	for _, cur := range c.cursors {
		tmp := *kv
		found, err := cur.Find(&tmp)
		if err != nil {
			return false, err
		}
		if found == reader.Found {
			*kv = tmp
			return true, nil
		}
	}
	return false, nil
}

// returns Found for exact match
// Partial for found a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(kv *shared.KV) (int, error) {
	c.heap.Values = nil
	for i, cur := range c.cursors {
		key := Position{
			Cursor: cur,
			Index:  i,
		}
		key.KV.Key = kv.Key
		found, err := key.Cursor.Find(&key.KV)
		if err != nil {
			return 0, err
		}
		if found > 0 {
			c.heap.Values = append(c.heap.Values, key)
		}
	}

	c.heap.Init(1)
	if len(c.heap.Values) == 0 {
		return reader.NotFound, nil
	}

	v := &c.heap.Values[0]
	found := bytes.Equal(v.KV.Key, kv.Key)
	*kv = v.KV
	if found {
		return reader.Found, nil
	}
	return reader.Partial, nil
}
