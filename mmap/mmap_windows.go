/*Copyright (c) 2011, Evan Shaw <edsrzf@gmail.com>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.*/

package mmap

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/windows"
)

// mmap on Windows is a two-step process.
// First, we call CreateFileMapping to get a handle.
// Then, we call MapviewToFile to get an actual pointer into memory.
// Because we want to emulate a POSIX-style mmap, we don't want to expose
// the handle -- only the pointer. We also want to return only a byte slice,
// not a struct, so it's convenient to manipulate.

// We keep this map so that we can get back the original handle from the memory address.

type addrinfo struct {
	file     windows.Handle
	mapview  windows.Handle
	writable bool
}

var handleLock sync.Mutex
var handleMap = map[uintptr]*addrinfo{}

func mmap(len int, prot, flags, hfile uintptr, off int64) ([]byte, error) {
	flProtect := uint32(windows.PAGE_READONLY)
	dwDesiredAccess := uint32(windows.FILE_MAP_READ)
	writable := false
	switch {
	case prot&COPY != 0:
		flProtect = windows.PAGE_WRITECOPY
		dwDesiredAccess = windows.FILE_MAP_COPY
		writable = true
	case prot&RDWR != 0:
		flProtect = windows.PAGE_READWRITE
		dwDesiredAccess = windows.FILE_MAP_WRITE
		writable = true
	}
	if prot&EXEC != 0 {
		flProtect <<= 4
		dwDesiredAccess |= windows.FILE_MAP_EXECUTE
	}

	// The maximum size is the area of the file, starting from 0,
	// that we wish to allow to be mappable. It is the sum of
	// the length the user requested, plus the offset where that length
	// is starting from. This does not map the data into memory.
	maxSizeHigh := uint32((off + int64(len)) >> 32)
	maxSizeLow := uint32((off + int64(len)) & 0xFFFFFFFF)
	// TODO: Do we need to set some security attributes? It might help portability.
	h, errno := windows.CreateFileMapping(windows.Handle(hfile), nil, flProtect, maxSizeHigh, maxSizeLow, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Actually map a view of the data into memory. The view's size
	// is the length the user requested.
	fileOffsetHigh := uint32(off >> 32)
	fileOffsetLow := uint32(off & 0xFFFFFFFF)
	addr, errno := windows.MapViewOfFile(h, dwDesiredAccess, fileOffsetHigh, fileOffsetLow, uintptr(len))
	if addr == 0 {
		windows.CloseHandle(windows.Handle(h))
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}
	handleLock.Lock()
	handleMap[addr] = &addrinfo{
		file:     windows.Handle(hfile),
		mapview:  h,
		writable: writable,
	}
	handleLock.Unlock()

	m := MMap{}
	dh := m.header()
	dh.Data = addr
	dh.Len = len
	dh.Cap = dh.Len

	return m, nil
}

func (m MMap) flush() error {
	addr, len := m.addrLen()
	errno := windows.FlushViewOfFile(addr, len)
	if errno != nil {
		return os.NewSyscallError("FlushViewOfFile", errno)
	}

	handleLock.Lock()
	defer handleLock.Unlock()
	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've errored above
		return errors.New("unknown base address")
	}

	if handle.writable && handle.file != windows.Handle(^uintptr(0)) {
		if err := windows.FlushFileBuffers(handle.file); err != nil {
			return os.NewSyscallError("FlushFileBuffers", err)
		}
	}

	return nil
}

func (m MMap) lock() error {
	addr, len := m.addrLen()
	errno := windows.VirtualLock(addr, len)
	return os.NewSyscallError("VirtualLock", errno)
}

func (m MMap) unlock() error {
	addr, len := m.addrLen()
	errno := windows.VirtualUnlock(addr, len)
	return os.NewSyscallError("VirtualUnlock", errno)
}

func (m MMap) unmap() error {
	err := m.flush()
	if err != nil {
		return err
	}

	addr := m.header().Data
	// Lock the UnmapViewOfFile along with the handleMap deletion.
	// As soon as we unmap the view, the OS is free to give the
	// same addr to another new map. We don't want another goroutine
	// to insert and remove the same addr into handleMap while
	// we're trying to remove our old addr/handle pair.
	handleLock.Lock()
	defer handleLock.Unlock()
	err = windows.UnmapViewOfFile(addr)
	if err != nil {
		return err
	}

	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've errored above
		return errors.New("unknown base address")
	}
	delete(handleMap, addr)

	e := windows.CloseHandle(windows.Handle(handle.mapview))
	return os.NewSyscallError("CloseHandle", e)
}