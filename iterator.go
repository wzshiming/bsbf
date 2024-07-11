package bsbf

type keyAndValue struct {
	keySepFunc KeySepFunc
	raw        []byte
	key        []byte
	value      []byte
}

func (k *keyAndValue) init() {
	if k.raw == nil {
		return
	}

	k.key, k.value = k.keySepFunc(k.raw)
	k.raw = nil
}

func (k *keyAndValue) Key() []byte {
	k.init()
	return k.key
}

func (k *keyAndValue) Value() []byte {
	k.init()
	return k.value
}

type Iterator struct {
	b  *BSBF
	r  Range
	kv keyAndValue
}

func (i *Iterator) Range() Range {
	return i.r
}

func (i *Iterator) Key() []byte {
	return i.kv.Key()
}

func (i *Iterator) Value() []byte {
	return i.kv.Value()
}

func (i *Iterator) Next() (*Iterator, bool) {
	return i.b.next(i.r.End)
}

func (i *Iterator) Previous() (*Iterator, bool) {
	return i.b.previous(i.r.Begin)
}

func (b *BSBF) next(begin int64) (*Iterator, bool) {
	if begin == int64(len(b.data)) {
		return nil, false
	}
	end, endOff := seekLineAfter(b.data, b.bufSize, b.lineSep, begin)

	return &Iterator{
		b: b,
		r: Range{
			Begin: begin,
			End:   end,
		},
		kv: keyAndValue{
			raw:        b.data[begin:endOff],
			keySepFunc: b.keySepFunc,
		},
	}, true
}

func (b *BSBF) previous(end int64) (*Iterator, bool) {
	end -= int64(len(b.lineSep))
	if end < 0 {
		return nil, false
	}
	begin := seekLineBefore(b.data, b.bufSize, b.lineSep, end)

	return &Iterator{
		b: b,
		r: Range{
			Begin: begin,
			End:   end,
		},
		kv: keyAndValue{
			raw:        b.data[begin:end],
			keySepFunc: b.keySepFunc,
		},
	}, true
}

func (b *BSBF) First() (*Iterator, bool) {
	return b.next(0)
}

func (b *BSBF) Last() (*Iterator, bool) {
	return b.previous(int64(len(b.data)))
}
