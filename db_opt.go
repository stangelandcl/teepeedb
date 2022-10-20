package db

import (
	"github.com/stangelandcl/teepeedb/reader"
	"github.com/stangelandcl/teepeedb/writer"
)

type Opt struct {
	w     writer.Opt
	cache Cache
}

type Cache interface {
	reader.Cache
}

func NewCache(size int) Cache {
	return reader.NewCache(size)
}

func (opt Opt) WithCache(cache Cache) Opt {
	opt.cache = cache
	return opt
}

func (opt Opt) WithCacheSize(size int) Opt {
	opt.cache = NewCache(size)
	return opt
}

func NewOpt() Opt {
	return Opt{
		w: writer.NewOpt(),
	}
}
