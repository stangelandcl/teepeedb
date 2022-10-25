/*
	Conversion of standard library heap.go to concrete types

Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

  - Redistributions of source code must retain the above copyright

notice, this list of conditions and the following disclaimer.
  - Redistributions in binary form must reproduce the above

copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
  - Neither the name of Google Inc. nor the names of its

contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package merge

import (
	"bytes"

	"github.com/stangelandcl/teepeedb/internal/reader"
	"github.com/stangelandcl/teepeedb/internal/shared"
)

type Position struct {
	KV     shared.KV
	Cursor *reader.Cursor
	Index  int
}

type heap struct {
	Values []Position
	Order  int // 1 for normal sort, -1 for reverse
}

// Init establishes the heap invariants required by the other routines in this package.
// Init is idempotent with respect to the heap invariants
// and may be called whenever the heap invariants may have been invalidated.
// The complexity is O(n) where n = h.Len().
func (h *heap) Init(order int) {
	h.Order = order
	// heapify
	n := len(h.Values)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func (h *heap) Push(x Position) {
	h.Values = append(h.Values, x)
	h.up(len(h.Values) - 1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func (h *heap) Pop() Position {
	n := len(h.Values) - 1
	h.Values[0], h.Values[n] = h.Values[n], h.Values[0]
	h.down(0, n)
	key := h.Values[len(h.Values)-1]
	h.Values = h.Values[:len(h.Values)-1]
	return key
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
// Changing the value of the element at index i and then calling Fix is equivalent to,
// but less expensive than, calling Remove(h, i) followed by a Push of the new value.
// The complexity is O(log n) where n = h.Len().
func (h *heap) Fix(i int) {
	if !h.down(i, len(h.Values)) {
		h.up(i)
	}
}

func (h *heap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j {
			break
		} // parent
		c := bytes.Compare(h.Values[j].KV.Key, h.Values[i].KV.Key) * h.Order
		if c > 0 { // not less
			break
		}
		if c == 0 {
			if h.Values[j].Index >= h.Values[i].Index { // not less
				break
			}
		}
		h.Values[i], h.Values[j] = h.Values[j], h.Values[i]
		j = i
	}
}

func (h *heap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1
		j2 := j1 + 1 // left child
		if j2 < n {
			c := bytes.Compare(h.Values[j2].KV.Key, h.Values[j1].KV.Key) * h.Order
			if c < 0 || (c == 0 && h.Values[j2].Index < h.Values[j1].Index) { // less
				j = j2 // = 2*i + 2  // right child
			}
		}
		c := bytes.Compare(h.Values[j].KV.Key, h.Values[i].KV.Key) * h.Order
		if c > 0 { // not less
			break
		}
		if c == 0 {
			if h.Values[j].Index >= h.Values[i].Index { // not less
				break
			}
		}
		h.Values[i], h.Values[j] = h.Values[j], h.Values[i]
		i = j
	}
	return i > i0
}
