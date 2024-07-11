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

type KeySepFunc func(a []byte) ([]byte, []byte)

type TrimFunc func(a []byte) []byte

type BSBF struct {
	cmpFunc    CmpFunc
	keySepFunc KeySepFunc
	trimFunc   TrimFunc

	path string

	lineSep []byte

	bufSize int64

	cache      map[int64]*searchCacheItem
	cacheLevel int

	file *os.File
	data mmap

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

func WithTrimFunc(t TrimFunc) Option {
	return func(o *BSBF) {
		o.trimFunc = t
	}
}

func WithCacheLevel(b int) Option {
	return func(o *BSBF) {
		if b != 0 {
			o.cache = make(map[int64]*searchCacheItem)
			o.cacheLevel = b
		} else {
			o.cache = nil
			o.cacheLevel = b
		}
	}
}

func WithPath(path string) Option {
	return func(o *BSBF) {
		o.path = path
	}
}

func WithData(data []byte) Option {
	return func(o *BSBF) {
		o.data = data
	}
}

func NewBSBF(opts ...Option) *BSBF {
	b := &BSBF{
		cmpFunc:    Compare,
		lineSep:    []byte("\n"),
		keySepFunc: KeySeparator([]byte(" ")),
		bufSize:    2 * 1024,
	}
	for _, opt := range opts {
		opt(b)
	}

	if b.trimFunc != nil {
		b.data = b.trimFunc(b.data)
	}
	return b
}

func (b *BSBF) Sort(sizeFile int) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	err := b.loadFile()
	if err != nil {
		return err
	}

	if b.file == nil {
		return nil
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

func (b *BSBF) Reload() error {
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.file == nil {
		return nil
	}

	if b.data != nil {
		b.resetFile()
	}
	return b.loadFile()
}

func (b *BSBF) loadFile() error {
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

	data, err := newMmap(f, 0, int(size))
	if err != nil {
		return err
	}

	if b.trimFunc != nil {
		data = b.trimFunc(data)
	}

	b.data = data
	b.file = f
	return nil
}

func (b *BSBF) Search(key []byte) (*Iterator, bool, error) {
	b.mut.Lock()
	defer b.mut.Unlock()

	err := b.loadFile()
	if err != nil {
		return nil, false, err
	}
	r, k, v, ok, err := b.search(key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	iter := &Iterator{
		b: b,
		r: r,
		kv: keyAndValue{
			key:   k,
			value: v,
		},
	}
	return iter, true, nil
}
