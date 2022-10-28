package reader

import (
	"fmt"

	"github.com/stangelandcl/teepeedb/internal/shared"
)

type Cursor struct {
	f         BlockReader
	fixedSize int
	block     Block
	indexes   []Index
}

func (c *Cursor) First(kv *shared.KV) bool {
	return c.firstLast(First, kv)
}

func (c *Cursor) Last(kv *shared.KV) bool {
	return c.firstLast(Last, kv)
}

func (c *Cursor) Next(kv *shared.KV) bool {
	return c.nextPrev(Next, kv)
}

func (c *Cursor) Previous(kv *shared.KV) bool {
	return c.nextPrev(Previous, kv)
}

func (c *Cursor) Move(dir Move, kv *shared.KV) bool {
	switch dir {
	case First, Last:
		return c.firstLast(dir, kv)
	case Next, Previous:
		return c.nextPrev(dir, kv)
	}
	return false
}

func (c *Cursor) Find(kv *shared.KV) FindResult {
	if c.block.InRange(kv) {
		return c.block.Find(kv, false)
	}

	if true {
		for i := len(c.indexes) - 1; i > 0; i-- {
			idx := c.indexes[i]
			if idx.InRange(kv) {
				break
			}
			c.indexes = c.indexes[:len(c.indexes)-1]
		}
	} else {
		c.indexes = c.indexes[:1]
	}
	ikv := IndexKV{}
	for i := len(c.indexes) - 1; i < len(c.indexes); i++ {
		ikv.Key = kv.Key
		if !c.indexes[i].LessOrEqual(&ikv) {
			return NotFound
		}
		if ikv.Type == shared.DataBlock {
			break
		}

		buf := c.f.ReadBlock(ikv.Position)
		idx := NewIndex(buf)
		c.indexes = append(c.indexes, idx)
	}

	if !c.block.Match(ikv.Position) {
		buf := c.f.ReadBlock(ikv.Position)
		c.block = NewBlock(buf, c.fixedSize, ikv.Position)
	}
	return c.block.Find(kv, false)
}

func (c *Cursor) follow(dir Move, ikv *IndexKV, i int) bool {
	for ; i < len(c.indexes); i++ {
		if !c.indexes[i].Move(dir, ikv) {
			return false
		}
		if ikv.Type == shared.DataBlock {
			c.indexes = c.indexes[:i+1]
			break
		}
		buf := c.f.ReadBlock(ikv.Position)
		idx := NewIndex(buf)
		idx.Move(dir, ikv)
		c.indexes = append(c.indexes, idx)
	}

	if !c.block.Match(ikv.Position) {
		buf := c.f.ReadBlock(ikv.Position)
		c.block = NewBlock(buf, c.fixedSize, ikv.Position)
	}
	return true
}

func (c *Cursor) firstLast(dir Move, kv *shared.KV) bool {
	c.indexes = c.indexes[:1]
	ikv := IndexKV{}
	found := c.follow(dir, &ikv, 0)
	if !found {
		return found
	}
	return c.block.Move(dir, kv)
}

func (c *Cursor) nextPrev(dir Move, kv *shared.KV) bool {
	if c.block.Move(dir, kv) {
		return true
	}

	ikv := IndexKV{}
	i := len(c.indexes) - 1
	for ; i >= 0; i-- {
		if c.indexes[i].Move(dir, &ikv) {
			break
		}
	}
	if i < 0 {
		return false
	}
	c.indexes = c.indexes[:i+1]
	if ikv.Type == shared.IndexBlock {
		buf := c.f.ReadBlock(ikv.Position)
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
	return c.block.Move(dir, kv)
}

func (c *Cursor) Print() {
	for i := 0; i < len(c.indexes); i++ {
		fmt.Printf("------ idx %v ------\n", i)
		c.indexes[i].Print()
	}
	fmt.Println("------ data ------")
	c.block.Print()
}
