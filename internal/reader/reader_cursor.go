package reader

import (
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type Cursor struct {
	r       *File
	block   Block
	indexes []Index
}

func (c *Cursor) Key() ([]byte, bool) {
	return c.block.rb.Key(c.block.idx)
}

func (c *Cursor) Value() []byte {
	return c.block.Value(c.block.idx)
}

func (c *Cursor) First() bool {
	return c.firstLast(First)
}

func (c *Cursor) Last() bool {
	return c.firstLast(Last)
}

func (c *Cursor) Next() bool {
	return c.nextPrev(Next)
}

func (c *Cursor) Previous() bool {
	return c.nextPrev(Previous)
}

func (c *Cursor) Find(key []byte) FindResult {
	if c.block.InRange(key) {
		return c.block.Find(key, false)
	}
	for i := len(c.indexes) - 1; i > 0; i-- {
		idx := c.indexes[i]
		if idx.InRange(key) {
			break
		}
		c.indexes[i].b.Close()
		c.indexes = c.indexes[:i]
	}
	ikv := IndexKV{}
	ikv.Position = -1
	for i := len(c.indexes) - 1; i < len(c.indexes); i++ {
		if !c.indexes[i].LessOrEqual(key) {
			return NotFound
		}

		ikv = c.indexes[i].Get()
		if ikv.Type == shared.DataBlock {
			break
		}

		buf := c.r.readBlock(ikv.Position)
		idx := NewIndex(buf)
		c.indexes = append(c.indexes, idx)
	}

	if !c.block.Match(ikv.Position) {
		c.block.Close()
		buf := c.r.readBlock(ikv.Position)
		c.block = NewBlock(buf, ikv.Position)
	}
	return c.block.Find(key, false)
}

func (c *Cursor) follow(dir Move, ikv *IndexKV, i int) bool {
	for ; i < len(c.indexes); i++ {
		if !c.indexes[i].Move(dir) {
			return false
		}
		*ikv = c.indexes[i].Get()
		if ikv.Type == shared.DataBlock {
			for j := i + 1; j < len(c.indexes); j++ {
				c.indexes[j].b.Close()
			}
			c.indexes = c.indexes[:i+1]
			break
		}
		buf := c.r.readBlock(ikv.Position)
		idx := NewIndex(buf)
		c.indexes = append(c.indexes, idx)
	}

	if !c.block.Match(ikv.Position) {
		c.block.Close()
		buf := c.r.readBlock(ikv.Position)
		c.block = NewBlock(buf, ikv.Position)
	}
	return true
}

func (c *Cursor) firstLast(dir Move) bool {
	if len(c.indexes) == 0 {
		// no data in file
		return false
	}
	for i := 1; i < len(c.indexes); i++ {
		c.indexes[i].b.Close()
	}
	c.indexes = c.indexes[:1]
	ikv := IndexKV{}
	found := c.follow(dir, &ikv, 0)
	if !found {
		return found
	}
	return c.block.Move(dir)
}

func (c *Cursor) nextPrev(dir Move) bool {
	if c.block.Move(dir) {
		return true
	}

	i := len(c.indexes) - 1
	for ; i >= 0; i-- {
		if c.indexes[i].Move(dir) {
			break
		}
	}
	if i < 0 {
		return false
	}
	for j := i + 1; j < len(c.indexes); j++ {
		c.indexes[j].b.Close()
	}
	c.indexes = c.indexes[:i+1]
	ikv := c.indexes[len(c.indexes)-1].Get()
	if ikv.Type == shared.IndexBlock {
		buf := c.r.readBlock(ikv.Position)
		c.indexes = append(c.indexes, NewIndex(buf))
	}
	switch dir {
	case Previous:
		dir = Last
	case Next:
		dir = First
	}
	found := c.follow(dir, &ikv, i+1)
	if !found {
		return false
	}
	return c.block.Move(dir)
}
