package shared

import "encoding/binary"

type BlockType byte

type BlockFormat byte

const (
	DataBlock  BlockType = 0
	IndexBlock BlockType = 1

	Raw BlockFormat = 0
	Lz4 BlockFormat = 1
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

type FileFooter struct {
	BlockSize         int
	BlockFormat       BlockFormat
	DataBlocks        int
	DataBytes         int
	Deletes           int
	IndexBlocks       int
	IndexBytes        int
	Inserts           int
	LastIndexPosition int
	ValueSize         int
}

func (h *FileFooter) Marshal() []byte {
	buf := make([]byte, 9*8+1) // fields x sizeof(uint64) + compression
	i := 0
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.BlockSize))
	i += 8
	buf[i] = byte(h.BlockFormat)
	i++
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.DataBlocks))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.DataBytes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.Deletes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.IndexBlocks))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.IndexBytes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.Inserts))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.LastIndexPosition))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.ValueSize))
	i += 8
	return buf
}

func (h *FileFooter) Unmarshal(buf []byte) {
	i := 0
	h.BlockSize = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.BlockFormat = BlockFormat(buf[i])
	i++
	h.DataBlocks = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.DataBytes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.Deletes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.IndexBlocks = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.IndexBytes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.Inserts = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.LastIndexPosition = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.ValueSize = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
}
