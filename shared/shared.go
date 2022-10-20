package shared

type BlockType byte

type Compression byte

const (
	DataBlock  BlockType = 0
	IndexBlock BlockType = 1

	Raw Compression = 0
	Lz4 Compression = 1
)

type IndexValue struct {
	LastKey  []byte
	Position int
	Type     BlockType
}

type KV struct {
	Key    []byte
	Value  []byte
	Delete bool
}
