package teepeedb

import (
	"github.com/stangelandcl/teepeedb/internal/merge"
	"github.com/stangelandcl/teepeedb/internal/reader"
)

type FindResult int

const (
	// no values greater or equal to key exist
	NotFound FindResult = FindResult(reader.NotFound)
	// exact match
	Found FindResult = FindResult(reader.Found)
	// found a value greater or equal to key
	FoundGreater FindResult = FindResult(reader.FoundGreater)
)

// Not found and nothing greater found
func (r FindResult) Empty() bool {
	return r == NotFound
}

// found either an exact match or a value greater than key
func (r FindResult) Any() bool {
	return r != NotFound
}

type Cursor struct {
	m *merge.Cursor
}

type KV struct {
	Key, Value []byte
}

func (c *Cursor) Close() {
	c.m.Close()
}

// call First or Find once before Previous
// if more is true kv is valid until next call to cursor function
func (c *Cursor) Next() bool {
	for {
		more := c.m.Next()
		if !more {
			return false
		}
		if !c.m.Delete {
			break
		}
	}
	return true
}

// call Last or Find once before Previous
// if more is true kv is valid until next call to cursor function
func (c *Cursor) Previous() bool {

	for {
		more := c.m.Previous()
		if !more {
			return false
		}
		if !c.m.Delete {
			break
		}
	}
	return true
}

// go to first key-value pair and return it if result is true
// if result is false then DB is empty
func (c *Cursor) First() bool {
	more := c.m.First()
	for more && c.m.Delete {
		more = c.m.Next()
	}
	return more
}

// go to last key-value pair and return it if result is true
// if result is false then DB is empty
func (c *Cursor) Last() bool {
	more := c.m.Last()
	for more && c.m.Delete {
		more = c.m.Previous()
	}
	return more
}

// set key on input.
// key and value will be set on output if found or partial is true
// returns Found for exact match
// FoundGreater for a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(find []byte) FindResult {
	rs := c.m.Find(find)
	result := FindResult(rs)
	if result == NotFound {
		return result
	}

	for c.m.Delete {
		var more bool
		more = c.m.Next()
		if !more {
			return NotFound
		}
		result = FoundGreater
	}
	return result
}

func (c *Cursor) Key() []byte {
	return c.m.Key
}

func (c *Cursor) Value() []byte {
	return c.m.Value()
}
