package writer

import (
	"encoding/binary"
	"io"

	"github.com/stangelandcl/teepeedb/shared"
)

type Opt struct {
	// size of each block in the file
	BlockSize int
	// < 0 == variable size value, 0 == key only, > 0 == fixed size value
	FixedValueSize int
	Compressed     bool
}

type File struct {
	indexes        []Index
	f              *Buffered
	blockWriter    BlockWriter
	block          Block
	blockSize      int
	fixedValueSize int

	indexSize         int
	indexCount        int
	dataSize          int
	dataCount         int
	lastIndexPosition int
	flushed           bool
	compressed        bool
}

func NewOpt() Opt {
	return Opt{
		BlockSize:      4096,
		FixedValueSize: -1,
		Compressed:     false,
	}
}

func NewFile(filename string, opts ...Opt) (*File, error) {
	opt := NewOpt()
	if len(opts) > 0 {
		opt = opts[0]
	}

	fw := &File{
		blockSize:      opt.BlockSize,
		fixedValueSize: opt.FixedValueSize,
	}
	f, err := NewBuffered(filename)
	if err != nil {
		return nil, err
	}
	fw.f = f
	fw.compressed = opt.Compressed
	if opt.Compressed {
		fw.blockWriter = NewLz4(fw.f)
	} else {
		fw.blockWriter = NewRaw(fw.f)
	}
	fw.block = NewBlock(opt.BlockSize, opt.FixedValueSize >= 0)
	fw.indexes = append(fw.indexes, NewIndex(fw.blockSize))
	return fw, nil
}

func (f *File) Len() int {
	return f.dataSize + f.indexSize
}

func (f *File) flush() error {
	pos := f.f.Position
	info, err := f.block.Write(f.blockWriter)
	if err != nil && err != io.EOF {
		return err
	}

	if err == nil {
		f.dataSize += f.f.Position - pos
		f.dataCount++
		key := info.FirstKey
		iInfo := shared.IndexValue{
			LastKey:  info.LastKey,
			Position: pos,
			Type:     shared.DataBlock,
		}
		f.addToIndex(key, iInfo, 0)
	}

	for i := 0; i < len(f.indexes); i++ {
		pos := f.f.Position
		info, err := f.indexes[i].Write(f.blockWriter)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			continue // already flushed
		}
		f.indexSize += f.f.Position - pos
		f.indexCount++
		f.lastIndexPosition = pos

		iInfo := shared.IndexValue{
			LastKey:  info.LastKey,
			Position: pos,
			Type:     shared.IndexBlock,
		}
		err = f.addToIndex(info.FirstKey, iInfo, i+1)
		if err != nil {
			return err
		}
	}

	vi := make([]byte, 17)
	var typ shared.CompressType
	if f.compressed {
		typ = shared.Lz4
	} else {
		typ = shared.Raw
	}
	vi[0] = byte(typ)
	binary.LittleEndian.PutUint64(vi[1:], uint64(f.fixedValueSize))
	binary.LittleEndian.PutUint64(vi[9:], uint64(f.lastIndexPosition))
	_, err = f.f.Write(vi)
	if err != nil {
		return err
	}

	f.flushed = true
	return nil
}

func (f *File) Close() error {
	var err1 error
	if !f.flushed {
		err1 = f.flush()
		// always call file.Close()
	}
	err2 := f.f.Close()
	if err2 != nil {
		return err2
	}
	return err1
}

func (f *File) Add(kv *shared.KV) error {
	if f.block.HasSpace(len(kv.Key), len(kv.Value)) {
		f.block.Add(kv)
		return nil
	}

	pos := f.f.Position
	info, err := f.block.Write(f.blockWriter)
	if err != nil {
		return err
	}
	f.dataSize += f.f.Position - pos
	f.dataCount++

	key := info.FirstKey
	iInfo := shared.IndexValue{
		LastKey:  info.LastKey,
		Position: pos,
		Type:     shared.DataBlock,
	}
	err = f.addToIndex(key, iInfo, 0)
	if err != nil {
		return err
	}
	f.block.Add(kv)
	return nil
}

func (f *File) addToIndex(key []byte, iInfo shared.IndexValue, i int) error {
	for ; i < len(f.indexes); i++ {
		if f.indexes[i].HasSpace(key, iInfo) {
			f.indexes[i].Add(key, iInfo)
			return nil
		}

		pos := f.f.Position
		info, err := f.indexes[i].Write(f.blockWriter)
		if err != nil {
			return err
		}
		f.indexSize += f.f.Position - pos
		f.indexCount++
		f.lastIndexPosition = pos

		f.indexes[i].Add(key, iInfo)
		key = info.FirstKey
		iInfo.Position = pos
		iInfo.LastKey = info.LastKey
		iInfo.Type = shared.IndexBlock

		if i == len(f.indexes)-1 {
			f.indexes = append(f.indexes, NewIndex(f.blockSize))
		}
	}
	return nil
}
