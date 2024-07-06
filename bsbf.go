package bsbf

import (
	"os"
	"path"
	"sync"
)

type Range struct {
	Begin int64
	End   int64
}

type CmpFunc func(a, b []byte) int

type KeySepFunc func(a []byte) ([]byte, []byte, bool)

type BSBF struct {
	cmpFunc    CmpFunc
	keySepFunc KeySepFunc

	path string

	lineSep []byte

	bufSize int64

	cache map[int64]*searchCacheItem

	file *os.File
	data mmap
	size int64

	mut sync.Mutex
}

type Option func(*BSBF)

func WithBufSize(size int64) Option {
	return func(o *BSBF) {
		o.bufSize = size
	}
}

func WithLineSep(lineSep []byte) Option {
	return func(o *BSBF) {
		o.lineSep = lineSep
	}
}

func WithKeySepFunc(ks KeySepFunc) Option {
	return func(o *BSBF) {
		o.keySepFunc = ks
	}
}

func WithCmpFunc(c CmpFunc) Option {
	return func(o *BSBF) {
		o.cmpFunc = c
	}
}

func WithSearchCache(b bool) Option {
	return func(o *BSBF) {
		if b {
			o.cache = make(map[int64]*searchCacheItem)
		} else {
			o.cache = nil
		}
	}
}

func NewBSBF(path string, opts ...Option) *BSBF {
	b := &BSBF{
		cmpFunc:    Compare,
		path:       path,
		lineSep:    []byte("\n"),
		keySepFunc: KeySeparator([]byte(" ")),
		bufSize:    2 * 1024,
	}
	for _, opt := range opts {
		opt(b)
	}

	return b
}

func (b *BSBF) Sort(sizeFile int) error {
	err := b.loadFile()
	if err != nil {
		return err
	}

	dir := path.Join(path.Dir(b.path), ".bsbf-tmp")
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	newFile := path.Join(dir, path.Base(b.path))
	err = sortSource(newFile, b.data, b.lineSep, sizeFile, b.keySepFunc, b.cmpFunc)
	if err != nil {
		return err
	}

	b.mut.Lock()
	defer b.mut.Unlock()
	b.resetFile()

	err = os.Rename(newFile, b.path)
	if err != nil {
		return err
	}
	return nil
}

func (b *BSBF) resetFile() {
	_ = b.data.Close()
	b.file.Close()
	b.data = nil
	return
}
func (b *BSBF) loadFile() error {
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.data != nil {
		return nil
	}

	f, err := os.Open(b.path)
	if err != nil {
		return err
	}

	s, err := f.Stat()
	if err != nil {
		return err
	}
	size := s.Size()

	m, err := newMmap(f, 0, int(size))
	if err != nil {
		return err
	}

	b.size = int64(len(m))
	b.data = m
	b.file = f
	return nil
}

func (b *BSBF) Search(key []byte) (Range, []byte, []byte, bool, error) {
	err := b.loadFile()
	if err != nil {
		return Range{}, nil, nil, false, err
	}
	b.mut.Lock()
	defer b.mut.Unlock()
	return b.search(key)
}
