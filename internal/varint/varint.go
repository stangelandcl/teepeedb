package varint

import "math/bits"

func Len(x int) int {
	// protobuf does this calculation. 6.5 times faster than LenSlow
	// 22 ms per 100 million vs 130 ms / 100 million
	bits := 63 ^ bits.LeadingZeros64(uint64(x)|1)
	return (bits*9 + 73) / 64
}

func LenSlow(x int) int {
	n := 0
	for {
		x >>= 7
		n++
		if x == 0 {
			break
		}
	}
	return n
}

func Read(buf []byte, i *int) int {
	bits := 0
	value := 0
	for {
		n := buf[*i]
		*i = *i + 1
		value += int(n&0x7F) << bits
		if n&128 == 0 {
			break
		}
		bits += 7
	}
	return value
}
