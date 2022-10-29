package writer

import (
	"encoding/binary"

	"github.com/stangelandcl/teepeedb/internal/block"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type File struct {
	indexes     []Index
	f           *Buffered
	blockWriter block.Writer
	block       block.WriteBlock
	footer      shared.FileFooter
}

func NewFile(filename string, blockSize int) (*File, error) {
	fw := &File{
		footer: shared.FileFooter{
			BlockSize:   blockSize,
			ValueSize:   -1,
			BlockFormat: 1,
		},
	}
	f, err := NewBuffered(filename)
	if err != nil {
		return nil, err
	}
	fw.f = f
	fw.indexes = append(fw.indexes, Index{})
	return fw, nil
}

func (f *File) Len() int {
	return f.footer.DataBytes + f.footer.IndexBytes
}

func (f *File) Commit() error {
	pos := f.f.Position
	info, err := f.blockWriter.Write(f.f, &f.block)
	if err != nil && err != block.ErrEmpty {
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
		info, err := f.indexes[i].Write(f.f, &f.blockWriter)
		if err != nil {
			if err == block.ErrEmpty {
				continue // already flushed
			}
			return err
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

	err = f.f.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (f *File) Close() error {
	err := f.f.Close()
	return err
}

func (f *File) Add(kv *shared.KV) error {
	if kv.Delete {
		f.footer.Deletes++
	} else {
		f.footer.Inserts++
	}

	if f.block.HasSpace(len(kv.Key), len(kv.Value), f.footer.BlockSize) {
		f.block.Put(kv.Key, kv.Value, kv.Delete)
		return nil
	}

	pos := f.f.Position
	info, err := f.blockWriter.Write(f.f, &f.block)
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
	f.block.Put(kv.Key, kv.Value, kv.Delete)
	return nil
}

func (f *File) addToIndex(key []byte, iInfo shared.IndexValue, i int) error {
	for ; i < len(f.indexes); i++ {
		if f.indexes[i].HasSpace(key, iInfo, f.footer.BlockSize) {
			f.indexes[i].Add(key, iInfo)
			return nil
		}

		pos := f.f.Position
		info, err := f.indexes[i].Write(f.f, &f.blockWriter)
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
			f.indexes = append(f.indexes, Index{})
		}
	}
	return nil
}
