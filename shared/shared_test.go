package shared

import (
	"testing"
)

func TestFooter(t *testing.T) {
	x := FileFooter{
		BlockSize:         1,
		DataBlocks:        2,
		DataBytes:         3,
		Deletes:           4,
		IndexBlocks:       5,
		IndexBytes:        6,
		Inserts:           7,
		LastIndexPosition: 8,
		ValueSize:         9,
		Compression:       Lz4,
	}

	buf := x.Marshal()
	y := FileFooter{}
	y.Unmarshal(buf)

	if x.BlockSize != y.BlockSize {
		panic("blocksize")
	}
	if x.DataBlocks != y.DataBlocks {
		panic("datablocks")
	}
	if x.DataBytes != y.DataBytes {
		panic("databytes")
	}
	if x.Deletes != y.Deletes {
		panic("deletes")
	}
	if x.IndexBlocks != y.IndexBlocks {
		panic("indexblocks")
	}
	if x.IndexBytes != y.IndexBytes {
		panic("indexbytes")
	}
	if x.Inserts != y.Inserts {
		panic("inserts")
	}
	if x.LastIndexPosition != y.LastIndexPosition {
		panic("lastindexpos")
	}
	if x.ValueSize != y.ValueSize {
		panic("valuesize")
	}
	if x.Compression != y.Compression {
		panic("compression")
	}
}
