package mmap

import (
	"os"
	"syscall"
)

/**
	一个汇编的操作系统
 */
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return nil, syscall.EPLAN9
}

// Munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	return syscall.EPLAN9
}

// Madvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. Set the readahead flag to
// false if page references are expected in random order.
func madvise(b []byte, readahead bool) error {
	return syscall.EPLAN9
}

func msync(b []byte) error {
	return syscall.EPLAN9
}
