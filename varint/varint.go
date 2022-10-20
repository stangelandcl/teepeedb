package varint

func Len(x int) int {
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
