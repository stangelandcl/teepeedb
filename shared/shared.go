package shared

type BlockType byte

type CompressType byte

const (
	DataBlock  BlockType = 0
	IndexBlock BlockType = 1

	Raw CompressType = 0
	Lz4 CompressType = 1
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
