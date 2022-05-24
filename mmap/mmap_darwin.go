package mmap

/**
	Mac系统的mmap调用
 */
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// Munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	return unix.Munmap(b)
}

// This is required because the unix package does not support the madvise system call on OS X.
func madvise(b []byte, readahead bool) error {
	advice := unix.MADV_NORMAL
	if !readahead {
		advice = unix.MADV_RANDOM
	}

	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		return e1
	}
	return nil
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
