package lz4

/*
Copied from https://github.com/pierrec/lz4

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
)

func decodeBlock(dst, src []byte) (ret int) {
	// Restrict capacities so we don't read or write out of bounds.
	dst = dst[:len(dst):len(dst)]
	src = src[:len(src):len(src)]

	const hasError = -2

	if len(src) == 0 {
		return hasError
	}

	defer func() {
		if recover() != nil {
			ret = hasError
		}
	}()

	var si, di uint
	for si < uint(len(src)) {
		// Literals and match lengths (token).
		b := uint(src[si])
		si++

		// Literals.
		if lLen := b >> 4; lLen > 0 {
			switch {
			case lLen < 0xF && si+16 < uint(len(src)):
				// Shortcut 1
				// if we have enough room in src and dst, and the literals length
				// is small enough (0..14) then copy all 16 bytes, even if not all
				// are part of the literals.
				copy(dst[di:], src[si:si+16])
				si += lLen
				di += lLen
				if mLen := b & 0xF; mLen < 0xF {
					// Shortcut 2
					// if the match length (4..18) fits within the literals, then copy
					// all 18 bytes, even if not all are part of the literals.
					mLen += 4
					if offset := u16(src[si:]); mLen <= offset && offset < di {
						i := di - offset
						// The remaining buffer may not hold 18 bytes.
						// See https://github.com/pierrec/lz4/issues/51.
						if end := i + 18; end <= uint(len(dst)) {
							copy(dst[di:], dst[i:end])
							si += 2
							di += mLen
							continue
						}
					}
				}
			case lLen == 0xF:
				for {
					x := uint(src[si])
					if lLen += x; int(lLen) < 0 {
						return hasError
					}
					si++
					if x != 0xFF {
						break
					}
				}
				fallthrough
			default:
				copy(dst[di:di+lLen], src[si:si+lLen])
				si += lLen
				di += lLen
			}
		}

		mLen := b & 0xF
		if si == uint(len(src)) && mLen == 0 {
			break
		} else if si >= uint(len(src)) {
			return hasError
		}

		offset := u16(src[si:])
		if offset == 0 {
			return hasError
		}
		si += 2

		// Match.
		mLen += minMatch
		if mLen == minMatch+0xF {
			for {
				x := uint(src[si])
				if mLen += x; int(mLen) < 0 {
					return hasError
				}
				si++
				if x != 0xFF {
					break
				}
			}
		}

		// Copy the match.
		if di < offset {
			// The match is beyond our block, meaning the first part
			// is in the dictionary.
			log.Panicln("lz4: out of bounds")
			/*
				fromDict := dict[uint(len(dict))+di-offset:]
				n := uint(copy(dst[di:di+mLen], fromDict))
				di += n
				if mLen -= n; mLen == 0 {
					continue
				}
			*/
			// We copied n = offset-di bytes from the dictionary,
			// then set di = di+n = offset, so the following code
			// copies from dst[di-offset:] = dst[0:].
		}

		expanded := dst[di-offset:]
		if mLen > offset {
			// Efficiently copy the match dst[di-offset:di] into the dst slice.
			bytesToCopy := offset * (mLen / offset)
			for n := offset; n <= bytesToCopy+offset; n *= 2 {
				copy(expanded[n:], expanded[:n])
			}
			di += bytesToCopy
			mLen -= bytesToCopy
		}
		di += uint(copy(dst[di:di+mLen], expanded[:mLen]))
	}

	return int(di)
}

func u16(p []byte) uint { return uint(binary.LittleEndian.Uint16(p)) }
