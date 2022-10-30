package teepeedb

import (
	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/merge"
	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type FindResult int
type Which int

const (
	// no values greater or equal to key exist
	NotFound FindResult = FindResult(reader.NotFound)
	// exact match
	Found FindResult = FindResult(reader.Found)
	// found a value greater or equal to key
	FoundGreater FindResult = FindResult(reader.FoundGreater)

	Key   Which = Which(block.Key)
	Value Which = Which(block.Val)
	Both  Which = Which(block.Both)
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

/*
// set Key on input, value will be set if found is true
func (c *Cursor) Get(key []byte) bool {
	found, delete := c.m.Get(key)
	found = found && !delete
	return found
}
*/

// call First or Find once before Previous
// if more is true kv is valid until next call to cursor function
func (c *Cursor) Next() bool {
	for {
		more, delete := c.m.Next()
		if !more {
			return false
		}
		if !delete {
			break
		}
	}
	return true
}

// call Last or Find once before Previous
// if more is true kv is valid until next call to cursor function
func (c *Cursor) Previous() bool {

	for {
		more, delete := c.m.Previous()
		if !more {
			return false
		}
		if !delete {
			break
		}
	}
	return true
}

// go to first key-value pair and return it if result is true
// if result is false then DB is empty
func (c *Cursor) First() bool {
	more, delete := c.m.First()
	for more && delete {
		more, delete = c.m.Next()
	}
	return more
}

// go to last key-value pair and return it if result is true
// if result is false then DB is empty
func (c *Cursor) Last() bool {
	more, delete := c.m.Last()
	for more && delete {
		more, delete = c.m.Previous()
	}
	return more
}

// set key on input.
// key and value will be set on output if found or partial is true
// returns Found for exact match
// FoundGreater for a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(find []byte) FindResult {
	rs, delete := c.m.Find(find)
	result := FindResult(rs)
	if result == NotFound {
		return result
	}

	for delete {
		var more bool
		more, delete = c.m.Next()
		if !more {
			return NotFound
		}
		result = FoundGreater
	}
	return result
}

func (c *Cursor) Current(which Which, kv *shared.KV) {
	c.m.Current(block.Which(which), kv)
}
