package writer

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/stangelandcl/teepeedb/internal/shared"
)

type File struct {
	indexes     []Index
	f           *Buffered
	blockWriter BlockWriter
	block       Block
	footer      shared.FileFooter

	flushed bool
}

func NewFile(filename string, blockSize, valueSize int, compression shared.Compression) (*File, error) {
	fw := &File{
		footer: shared.FileFooter{
			BlockSize:   blockSize,
			ValueSize:   valueSize,
			Compression: compression,
		},
	}
	f, err := NewBuffered(filename)
	if err != nil {
		return nil, err
	}
	fw.f = f
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
	fw.indexes = append(fw.indexes, NewIndex(fw.footer.BlockSize))
	return fw, nil
}

func (f *File) Len() int {
	return f.footer.DataBytes + f.footer.IndexBytes
}

func (f *File) flush() error {
	pos := f.f.Position
	info, err := f.block.Write(f.blockWriter)
	if err != nil && err != io.EOF {
		return err
	}

	if err == nil {
		f.footer.DataBytes += f.f.Position - pos
		f.footer.DataBlocks++
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
		f.footer.IndexBytes += f.f.Position - pos
		f.footer.IndexBlocks++
		f.footer.LastIndexPosition = pos

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

	h := f.footer.Marshal()
	_, err = f.f.Write(h)
	if err != nil {
		return err
	}
	footerSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSize, uint32(len(h)))
	_, err = f.f.Write(footerSize)
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
	if kv.Delete {
		f.footer.Deletes++
	} else {
		f.footer.Inserts++
	}

	if f.block.HasSpace(len(kv.Key), len(kv.Value)) {
		f.block.Add(kv)
		return nil
	}

	pos := f.f.Position
	info, err := f.block.Write(f.blockWriter)
	if err != nil {
		return err
	}
	f.footer.DataBytes += f.f.Position - pos
	f.footer.DataBlocks++

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
		f.footer.IndexBytes += f.f.Position - pos
		f.footer.IndexBlocks++
		f.footer.LastIndexPosition = pos

		f.indexes[i].Add(key, iInfo)
		key = info.FirstKey
		iInfo.Position = pos
		iInfo.LastKey = info.LastKey
		iInfo.Type = shared.IndexBlock

		if i == len(f.indexes)-1 {
			f.indexes = append(f.indexes, NewIndex(f.footer.BlockSize))
		}
	}
	return nil
}
