package bsbf

import (
	"os"
	"syscall"
)

type mmap []byte

func newMmap(f *os.File, offset int64, length int) (mmap, error) {
	return syscall.Mmap(int(f.Fd()), offset, length, syscall.PROT_READ, syscall.MAP_SHARED)
}

func (m *mmap) Close() error {
	return syscall.Munmap(*m)
}
