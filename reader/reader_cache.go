package reader

import lru "github.com/hashicorp/golang-lru"

type Cache interface {
	Get(key any) (val any, ok bool)
	Add(key, val any)
}

func NewCache(size int) Cache {
	cache, err := lru.New2Q(size)
	if err != nil {
		panic(err)
	}
	return cache
}

type NullCache struct{}

func (c *NullCache) Get(key any) (val any, ok bool) {
	return nil, false
}

func (c *NullCache) Add(key, val any) {}
