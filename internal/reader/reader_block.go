package reader

import (
	"bytes"

	"github.com/stangelandcl/teepeedb/internal/block"
)

type Move byte
type FindResult int

const (
	// no values greater or equal to key exist
	NotFound FindResult = iota
	// exact match
	Found
	// found a value greater or equal to key
	FoundGreater
)

const (
	First    Move = 0
	Last     Move = 1
	Next     Move = 2
	Previous Move = 3
)

// readers are lightweight and can be recreated for each block read
type Block struct {
	rb       *block.ReadBlock
	idx      int // set to next value to read in forward iter order
	position int // position of block in file
}

// readers are lightweight and can be recreated for each block read
// fixedValueSize < 0 == variable length
// every reader needs its own decompressor
func NewBlock(rb *block.ReadBlock, position int) Block {
	b := Block{
		rb:       rb,
		idx:      -1,
		position: position,
	}
	return b
}

func (b *Block) Close() {
	if b.rb != nil {
		b.rb.Close()
		b.rb = nil
	}
}

func (b *Block) Find(find []byte, back bool) FindResult {
	lo := 0
	hi := b.rb.Count - 1

	for lo <= hi {
		i := (lo + hi) / 2
		k, _ := b.rb.Key(i)
		c := bytes.Compare(k, find)
		if c < 0 {
			lo = i + 1
		} else if c > 0 {
			hi = i - 1
		} else {
			b.idx = i
			return Found
		}
	}

	// return first value less than key
	b.idx = lo
	if back {
		if b.idx > 0 {
			b.idx--
		}
		return FoundGreater
	} else if b.idx < b.rb.Count {
		return FoundGreater
	}
	return NotFound
}

func (b *Block) Value(idx int) []byte {
	return b.rb.Value(idx)
}

func (b *Block) Len() int {
	return int(b.rb.Count)
}

// return true if block is loaded from same position
func (b *Block) Match(pos int) bool {
	return pos == b.position && b.rb != nil
}

func (b *Block) InRange(key []byte) bool {
	if b.rb == nil { // happens in Find() when nothing called before
		return false
	}

	k, _ := b.rb.Key(0)
	if bytes.Compare(key, k) < 0 {
		return false
	}
	k, _ = b.rb.Key(b.rb.Count - 1)
	return bytes.Compare(key, k) <= 0
}

func (b *Block) GoBack() {
	if b.idx > 0 {
		b.idx--
	}
}

func (b *Block) Move(m Move) bool {
	switch m {
	case First:
		b.idx = -1
		fallthrough
	case Next:
		more := b.idx+1 < b.rb.Count
		if more {
			b.idx++
		}
		return more
	case Last:
		b.idx = b.rb.Count
		fallthrough
	case Previous:
		more := b.idx != 0
		if more {
			b.idx--
		}
		return more
	}
	return false
}
