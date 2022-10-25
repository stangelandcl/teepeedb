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
func (c *Cursor) Get(kv *KV) (found bool, err error) {
	tmp := shared.KV{
		Key: kv.Key,
	}

	found, err = c.m.Get(&tmp)
	found = found && !tmp.Delete
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return
}

// if more is true kv is valid until next call to cursor function
func (c *Cursor) Next(kv *KV) (more bool, err error) {
	tmp := shared.KV{}

	for {
		more, err = c.m.Next(&tmp)
		if !more {
			return
		}
		if !tmp.Delete {
			break
		}
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return
}

// if more is true kv is valid until next call to cursor function
func (c *Cursor) Previous(kv *KV) (more bool, err error) {
	tmp := shared.KV{}

	for {
		more, err = c.m.Previous(&tmp)
		if !more {
			return
		}
		if !tmp.Delete {
			break
		}
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return
}

func (c *Cursor) First(kv *KV) (more bool, err error) {
	tmp := shared.KV{}

	more, err = c.m.First(&tmp)
	if !more {
		return
	}

	for tmp.Delete {
		more, err = c.m.Next(&tmp)
		if !more {
			return
		}
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return
}

func (c *Cursor) Last(kv *KV) (more bool, err error) {
	tmp := shared.KV{}
	more, err = c.m.Last(&tmp)
	if !more {
		return
	}

	for tmp.Delete {
		more, err = c.m.Previous(&tmp)
		if !more {
			return
		}
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return
}

// set key on input. value and key will be set if found or partial is true
// returns Found for exact match
// Partial for found a value greater than key.
// NotFound for no values >= key
func (c *Cursor) Find(kv *KV) (result FindResult, err error) {
	tmp := shared.KV{
		Key: kv.Key,
	}

	var r reader.FindResult
	r, err = c.m.Find(&tmp)
	result = FindResult(r)
	if err != nil || result == NotFound {
		return
	}

	for tmp.Delete {
		var more bool
		more, err = c.m.Next(&tmp)
		if !more {
			result = NotFound
			return
		}
		result = FoundGreater
	}
	kv.Key = tmp.Key
	kv.Value = tmp.Value
	return
}
