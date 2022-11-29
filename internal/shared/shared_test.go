package shared

import (
	"testing"
)

func TestFooter(t *testing.T) {
	x := FileFooter{
		BlockSize:            1,
		DataBlocks:           2,
		CompressedDataBytes:  3,
		Deletes:              4,
		IndexBlocks:          5,
		CompressedIndexBytes: 6,
		Inserts:              7,
		LastIndexPosition:    8,
		ValueSize:            9,
		BlockFormat:          1,
		RawKeyBytes:          10,
		RawValueBytes:        11,
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
	if x.CompressedDataBytes != y.CompressedDataBytes {
		panic("databytes")
	}
	if x.Deletes != y.Deletes {
		panic("deletes")
	}
	if x.IndexBlocks != y.IndexBlocks {
		panic("indexblocks")
	}
	if x.CompressedIndexBytes != y.CompressedIndexBytes {
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
	if x.BlockFormat != y.BlockFormat {
		panic("compression")
	}
	if x.RawKeyBytes != y.RawKeyBytes {
		panic("keybytes")
	}
	if x.RawValueBytes != y.RawValueBytes {
		panic("val bytes")
	}
}
