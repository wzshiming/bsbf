package bsbf

import (
	"os"
	"path"
	"sync"

	"github.com/wzshiming/mmap"
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

	mmap *mmap.MMap
	file *os.File
	data []byte

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

	if b.mmap == nil {
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

	_ = b.Close()

	err = os.Rename(newFile, b.path)
	if err != nil {
		return err
	}
	return nil
}

func (b *BSBF) Reload() error {
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.mmap == nil {
		return nil
	}

	if b.data != nil {
		_ = b.Close()
	}
	return b.loadFile()
}

func (b *BSBF) Close() error {
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.mmap == nil {
		return nil
	}

	err := b.mmap.Close()
	if err != nil {
		return err
	}

	err = b.file.Close()
	if err != nil {
		return err
	}

	b.mmap = nil
	b.file = nil
	return nil
}

func (b *BSBF) loadFile() error {
	if b.data != nil {
		return nil
	}

	f, err := os.Open(b.path)
	if err != nil {
		return err
	}

	m, err := mmap.Map(f, mmap.RDONLY)
	if err != nil {
		return err
	}

	data := m.Data()
	if b.trimFunc != nil {
		data = b.trimFunc(data)
	}

	b.data = data
	b.file = f
	b.mmap = m
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
