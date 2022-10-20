package writer

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/stangelandcl/teepeedb/shared"
)

type File struct {
	indexes     []Index
	f           *Buffered
	blockWriter BlockWriter
	block       Block
	blockSize   int
	valueSize   int

	indexSize         int
	indexCount        int
	dataSize          int
	dataCount         int
	lastIndexPosition int
	flushed           bool
	compression       shared.Compression
}

func NewFile(filename string, blockSize, valueSize int, compression shared.Compression) (*File, error) {
	fw := &File{
		blockSize: blockSize,
		valueSize: valueSize,
	}
	f, err := NewBuffered(filename)
	if err != nil {
		return nil, err
	}
	fw.f = f
	fw.compression = compression
	switch compression {
	case shared.Raw:
		fw.blockWriter = NewRaw(fw.f)
	case shared.Lz4:
		fw.blockWriter = NewLz4(fw.f)
	default:
		f.Close()
		os.Remove(filename)
		return nil, fmt.Errorf("teepeedb: unsupported compression: %v", compression)
	}
	fw.block = NewBlock(blockSize, valueSize >= 0)
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
	vi[0] = byte(f.compression)
	binary.LittleEndian.PutUint64(vi[1:], uint64(f.valueSize))
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
