package lz4

/*
Copied from https://github.com/pierrec/lz4 with errors replaced with panics

Copyright (c) 2015, Pierre Curto
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of xxHash nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import (
	"encoding/binary"
	"log"
	"math/bits"
	"sync"
)

const (
	// The following constants are used to setup the compression algorithm.
	minMatch   = 4  // the minimum size of the match sequence size (4 bytes)
	winSizeLog = 16 // LZ4 64Kb window size limit
	winSize    = 1 << winSizeLog
	winMask    = winSize - 1 // 64Kb window of previous data for dependent blocks

	// hashLog determines the size of the hash table used to quickly find a previous match position.
	// Its value influences the compression speed and memory usage, the lower the faster,
	// but at the expense of the compression ratio.
	// 16 seems to be the best compromise for fast compression.
	hashLog = 16
	htSize  = 1 << hashLog

	mfLimit = 10 + minMatch // The last match cannot start within the last 14 bytes.
)

// blockHash hashes the lower 6 bytes into a value < htSize.
func blockHash(x uint64) uint32 {
	const prime6bytes = 227718039650203
	return uint32(((x << (64 - 48)) * prime6bytes) >> (64 - hashLog))
}

func CompressBlockBound(n int) int {
	return n + n/255 + 16
}

func UncompressBlock(src, dst []byte) int {
	if len(src) == 0 {
		return 0
	}
	if di := decodeBlock(dst, src); di >= 0 {
		return di
	}
	log.Panicln("lz4: short src buffer")
	return 0
}

type Compressor struct {
	// Offsets are at most 64kiB, so we can store only the lower 16 bits of
	// match positions: effectively, an offset from some 64kiB block boundary.
	//
	// When we retrieve such an offset, we interpret it as relative to the last
	// block boundary si &^ 0xffff, or the one before, (si &^ 0xffff) - 0x10000,
	// depending on which of these is inside the current window. If a table
	// entry was generated more than 64kiB back in the input, we find out by
	// inspecting the input stream.
	table [htSize]uint16

	// Bitmap indicating which positions in the table are in use.
	// This allows us to quickly reset the table for reuse,
	// without having to zero everything.
	inUse [htSize / 32]uint32
}

// Get returns the position of a presumptive match for the hash h.
// The match may be a false positive due to a hash collision or an old entry.
// If si < winSize, the return value may be negative.
func (c *Compressor) get(h uint32, si int) int {
	h &= htSize - 1
	i := 0
	if c.inUse[h/32]&(1<<(h%32)) != 0 {
		i = int(c.table[h])
	}
	i += si &^ winMask
	if i >= si {
		// Try previous 64kiB block (negative when in first block).
		i -= winSize
	}
	return i
}

func (c *Compressor) put(h uint32, si int) {
	h &= htSize - 1
	c.table[h] = uint16(si)
	c.inUse[h/32] |= 1 << (h % 32)
}

func (c *Compressor) reset() { c.inUse = [htSize / 32]uint32{} }

var compressorPool = sync.Pool{New: func() interface{} { return new(Compressor) }}

func CompressBlock(src, dst []byte) int {
	c := compressorPool.Get().(*Compressor)
	n := c.CompressBlock(src, dst)
	compressorPool.Put(c)
	return n
}

func (c *Compressor) CompressBlock(src, dst []byte) int {
	// Zero out reused table to avoid non-deterministic output (issue #65).
	c.reset()

	// Return 0, nil only if the destination buffer size is < CompressBlockBound.
	isNotCompressible := len(dst) < CompressBlockBound(len(src))

	// adaptSkipLog sets how quickly the compressor begins skipping blocks when data is incompressible.
	// This significantly speeds up incompressible data and usually has very small impact on compression.
	// bytes to skip =  1 + (bytes since last match >> adaptSkipLog)
	const adaptSkipLog = 7

	// si: Current position of the search.
	// anchor: Position of the current literals.
	var si, di, anchor int
	sn := len(src) - mfLimit
	if sn <= 0 {
		goto lastLiterals
	}

	// Fast scan strategy: the hash table only stores the last 4 bytes sequences.
	for si < sn {
		// Hash the next 6 bytes (sequence)...
		match := binary.LittleEndian.Uint64(src[si:])
		h := blockHash(match)
		h2 := blockHash(match >> 8)

		// We check a match at s, s+1 and s+2 and pick the first one we get.
		// Checking 3 only requires us to load the source one.
		ref := c.get(h, si)
		ref2 := c.get(h2, si+1)
		c.put(h, si)
		c.put(h2, si+1)

		offset := si - ref

		if offset <= 0 || offset >= winSize || uint32(match) != binary.LittleEndian.Uint32(src[ref:]) {
			// No match. Start calculating another hash.
			// The processor can usually do this out-of-order.
			h = blockHash(match >> 16)
			ref3 := c.get(h, si+2)

			// Check the second match at si+1
			si += 1
			offset = si - ref2

			if offset <= 0 || offset >= winSize || uint32(match>>8) != binary.LittleEndian.Uint32(src[ref2:]) {
				// No match. Check the third match at si+2
				si += 1
				offset = si - ref3
				c.put(h, si)

				if offset <= 0 || offset >= winSize || uint32(match>>16) != binary.LittleEndian.Uint32(src[ref3:]) {
					// Skip one extra byte (at si+3) before we check 3 matches again.
					si += 2 + (si-anchor)>>adaptSkipLog
					continue
				}
			}
		}

		// Match found.
		lLen := si - anchor // Literal length.
		// We already matched 4 bytes.
		mLen := 4

		// Extend backwards if we can, reducing literals.
		tOff := si - offset - 1
		for lLen > 0 && tOff >= 0 && src[si-1] == src[tOff] {
			si--
			tOff--
			lLen--
			mLen++
		}

		// Add the match length, so we continue search at the end.
		// Use mLen to store the offset base.
		si, mLen = si+mLen, si+minMatch

		// Find the longest match by looking by batches of 8 bytes.
		for si+8 <= sn {
			x := binary.LittleEndian.Uint64(src[si:]) ^ binary.LittleEndian.Uint64(src[si-offset:])
			if x == 0 {
				si += 8
			} else {
				// Stop is first non-zero byte.
				si += bits.TrailingZeros64(x) >> 3
				break
			}
		}

		mLen = si - mLen
		if di >= len(dst) {
			log.Panicln("lz4: short buffer")
			return 0
		}
		if mLen < 0xF {
			dst[di] = byte(mLen)
		} else {
			dst[di] = 0xF
		}

		// Encode literals length.
		if lLen < 0xF {
			dst[di] |= byte(lLen << 4)
		} else {
			dst[di] |= 0xF0
			di++
			l := lLen - 0xF
			for ; l >= 0xFF && di < len(dst); l -= 0xFF {
				dst[di] = 0xFF
				di++
			}
			if di >= len(dst) {
				log.Panicln("lz4: short buffer")
				return 0
			}
			dst[di] = byte(l)
		}
		di++

		// Literals.
		if di+lLen > len(dst) {
			log.Panicln("lz4: short buffer")
			return 0
		}
		copy(dst[di:di+lLen], src[anchor:anchor+lLen])
		di += lLen + 2
		anchor = si

		// Encode offset.
		if di > len(dst) {
			log.Panicln("lz4: short buffer")
			return 0
		}
		dst[di-2], dst[di-1] = byte(offset), byte(offset>>8)

		// Encode match length part 2.
		if mLen >= 0xF {
			for mLen -= 0xF; mLen >= 0xFF && di < len(dst); mLen -= 0xFF {
				dst[di] = 0xFF
				di++
			}
			if di >= len(dst) {
				log.Panicln("lz4: short buffer")
				return 0
			}
			dst[di] = byte(mLen)
			di++
		}
		// Check if we can load next values.
		if si >= sn {
			break
		}
		// Hash match end-2
		h = blockHash(binary.LittleEndian.Uint64(src[si-2:]))
		c.put(h, si-2)
	}

lastLiterals:
	if isNotCompressible && anchor == 0 {
		// Incompressible.
		return 0
	}

	// Last literals.
	if di >= len(dst) {
		log.Panicln("lz4: short buffer")
		return 0
	}
	lLen := len(src) - anchor
	if lLen < 0xF {
		dst[di] = byte(lLen << 4)
	} else {
		dst[di] = 0xF0
		di++
		for lLen -= 0xF; lLen >= 0xFF && di < len(dst); lLen -= 0xFF {
			dst[di] = 0xFF
			di++
		}
		if di >= len(dst) {
			log.Panicln("lz4: short buffer")
			return 0
		}
		dst[di] = byte(lLen)
	}
	di++

	// Write the last literals.
	if isNotCompressible && di >= anchor {
		// Incompressible.
		return 0
	}
	if di+len(src)-anchor > len(dst) {
		log.Panicln("lz4: short buffer")
		return 0
	}
	di += copy(dst[di:di+len(src)-anchor], src[anchor:])
	return di
}

// blockHash hashes 4 bytes into a value < winSize.
func blockHashHC(x uint32) uint32 {
	const hasher uint32 = 2654435761 // Knuth multiplicative hash.
	return x * hasher >> (32 - winSizeLog)
}
