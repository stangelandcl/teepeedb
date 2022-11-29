package shared

import (
	"encoding/binary"
	"fmt"
)

type BlockType byte

const (
	DataBlock  BlockType = 0
	IndexBlock BlockType = 1
	// Max key size 32768 (16 bit key offsets - 1 bit for delete flag) / 8 = 4096.
	// slightly arbitrary but 4 keys must fit in less than 32768 bytes (minus some extra fluff)
	// because index has to hold 2 keys + the values for two keys which are also keys so 4 keys
	MaxKeySize = 4096 - 1 // -1 is arbitrary to keep less size in less than 12 bits
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
	BlockSize            int
	BlockFormat          int
	DataBlocks           int
	CompressedDataBytes  int
	Deletes              int
	IndexBlocks          int
	CompressedIndexBytes int
	Inserts              int
	LastIndexPosition    int
	ValueSize            int
	RawKeyBytes          int
	RawValueBytes        int
}

// Key was greater than shared.MaxKeySize
var ErrKeyTooBig = fmt.Errorf("teepee: key too big")

func (h *FileFooter) Marshal() []byte {
	buf := make([]byte, 12*8) // fields x sizeof(uint64)
	i := 0
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.BlockSize))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.BlockFormat))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.DataBlocks))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.CompressedDataBytes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.Deletes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.IndexBlocks))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.CompressedIndexBytes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.Inserts))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.LastIndexPosition))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.ValueSize))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.RawKeyBytes))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(h.RawValueBytes))
	i += 8
	return buf
}

func (h *FileFooter) Unmarshal(buf []byte) {
	i := 0
	h.BlockSize = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.BlockFormat = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.DataBlocks = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.CompressedDataBytes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.Deletes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.IndexBlocks = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.CompressedIndexBytes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.Inserts = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.LastIndexPosition = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.ValueSize = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	// other fields were added later. maintain backwards compatibility
	if i == len(buf) {
		h.RawKeyBytes = 0
		h.RawValueBytes = 0
		return
	}
	h.RawKeyBytes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
	h.RawValueBytes = int(binary.LittleEndian.Uint64(buf[i:]))
	i += 8
}
