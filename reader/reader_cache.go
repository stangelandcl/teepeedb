package reader

import (
	"log"

	lru "github.com/hashicorp/golang-lru"
)

type Cache interface {
	Get(key any) (val any, ok bool)
	Add(key, val any)
}

func NewCache(size int) Cache {
	if size <= 0 {
		size = 4 * 1024 * 1024
	}
	cache, err := lru.New2Q(size)
	if err != nil {
		// should never happen with size > 0
		log.Panicln("lru creation error", err)
	}
	return cache
}

type NullCache struct{}

func (c *NullCache) Get(key any) (val any, ok bool) {
	return nil, false
}

func (c *NullCache) Add(key, val any) {}
