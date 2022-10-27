//go:build darwin || dragonfly || freebsd || linux || openbsd || solaris || netbsd
// +build darwin dragonfly freebsd linux openbsd solaris netbsd

/*
From https://github.com/edsrzf/mmap-go with madvise read random call added

Copyright (c) 2011, Evan Shaw <edsrzf@gmail.com>
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
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

func mmap(len int, inprot, inflags, fd uintptr, off int64) ([]byte, error) {
	flags := unix.MAP_SHARED
	prot := unix.PROT_READ
	switch {
	case inprot&COPY != 0:
		prot |= unix.PROT_WRITE
		flags = unix.MAP_PRIVATE
	case inprot&RDWR != 0:
		prot |= unix.PROT_WRITE
	}
	if inprot&EXEC != 0 {
		prot |= unix.PROT_EXEC
	}
	if inflags&ANON != 0 {
		flags |= unix.MAP_ANON
	}

	b, err := unix.Mmap(int(fd), off, len, prot, flags)
	if err != nil {
		return nil, err
	}

	// from bbolt
	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		// Ignore not implemented error in kernel because it still works.
		return nil, fmt.Errorf("teepeedb: mmap madvise: %s", err)
	}
	return b, nil
}

func (m MMap) flush() error {
	return unix.Msync([]byte(m), unix.MS_SYNC)
}

func (m MMap) lock() error {
	return unix.Mlock([]byte(m))
}

func (m MMap) unlock() error {
	return unix.Munlock([]byte(m))
}

func (m MMap) unmap() error {
	return unix.Munmap([]byte(m))
}
