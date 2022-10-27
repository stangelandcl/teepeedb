package teepeedb

import (
	"github.com/stangelandcl/teepeedb/internal/merge"
	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
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

func (r FindResult) Empty() bool {
	return r == NotFound
}

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

// set Key on input, value will be set if found is true
func (c *Cursor) Get(kv *KV) bool {
	tmp := shared.KV{
		Key: kv.Key,
	}

	found := c.m.Get(&tmp)
	found = found && !tmp.Delete
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return found
}

// if more is true kv is valid until next call to cursor function
func (c *Cursor) Next(kv *KV) bool {
	tmp := shared.KV{}

	for {
		more := c.m.Next(&tmp)
		if !more {
			return false
		}
		if !tmp.Delete {
			break
		}
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return true
}

// if more is true kv is valid until next call to cursor function
func (c *Cursor) Previous(kv *KV) bool {
	tmp := shared.KV{}

	for {
		more := c.m.Previous(&tmp)
		if !more {
			return false
		}
		if !tmp.Delete {
			break
		}
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return true
}

func (c *Cursor) First(kv *KV) bool {
	tmp := shared.KV{}
	more := c.m.First(&tmp)
	for more && tmp.Delete {
		more = c.m.Next(&tmp)
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return more
}

func (c *Cursor) Last(kv *KV) bool {
	tmp := shared.KV{}
	more := c.m.Last(&tmp)
	for more && tmp.Delete {
		more = c.m.Previous(&tmp)
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return more
}

// set key on input. value and key will be set if found or partial is true
// returns Found for exact match
// Partial for found a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(kv *KV) FindResult {
	tmp := shared.KV{
		Key: kv.Key,
	}

	result := FindResult(c.m.Find(&tmp))
	if result == NotFound {
		return result
	}

	for tmp.Delete {
		var more bool
		more = c.m.Next(&tmp)
		if !more {
			return NotFound
		}
		result = FoundGreater
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return result
}
