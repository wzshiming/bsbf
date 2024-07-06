package bsbf

import (
	"bytes"
	"slices"
)

type searchCacheItem struct {
	Range Range
	Key   []byte
	Value []byte
}

type slicer interface {
	Slice(i, j int) ([]byte, error)
}

func (b *BSBF) search(key []byte) (Range, []byte, []byte, bool, error) {
	i, j := int64(0), b.size
	for i < j {
		off := int64(uint(i+j) >> 1)
		c, ok := b.cache[off]
		if !ok {
			rg, content, err := seekLine(b.data, b.size, b.bufSize, b.lineSep, off)
			if err != nil {
				return Range{}, nil, nil, false, err
			}

			k, v, ok := b.keySepFunc(content)
			if !ok {
				continue
			}

			c = searchCacheItem{
				Range: rg,
				Key:   k,
				Value: v,
			}

			b.cache[off] = c
		}

		switch b.cmpFunc(key, c.Key) {
		case 1:
			i = c.Range.End + 1
		case -1:
			j = c.Range.Begin
		default:
			return c.Range, c.Key, c.Value, true, nil
		}
	}
	return Range{}, nil, nil, false, nil
}

func seekLine(sl slicer, size int64, bufSize int64, lineSep []byte, off int64) (Range, []byte, error) {
	rg := Range{
		Begin: -1,
		End:   -1,
	}

	var output = make([][]byte, 0, 16)
	appendOutput := func(d []byte) {
		if len(d) == 0 {
			return
		}
		output = append(output, d)
	}

	if off == 0 {
		rg.Begin = off
	}

	for beginOff := off; rg.Begin == -1; {
		prevBeginOff := beginOff
		beginOff = max(0, beginOff-bufSize)
		limitBufSize := min(bufSize, off-beginOff)

		buf, err := sl.Slice(int(beginOff), int(beginOff+limitBufSize))
		if err != nil {
			return Range{}, nil, err
		}
		index := bytes.LastIndex(buf[:limitBufSize], lineSep)
		if index != -1 {
			appendOutput(buf[index+len(lineSep):])
			rg.Begin = prevBeginOff - (limitBufSize - int64(index)) + int64(len(lineSep))
		} else {
			if beginOff == 0 {
				rg.Begin = beginOff
			}
			appendOutput(buf)
		}
	}
	if len(output) >= 2 {
		slices.Reverse(output)
	}

	for endOff := off; rg.End == -1; {
		buf, err := sl.Slice(int(endOff), int(endOff+bufSize))
		if err != nil {
			return Range{}, nil, err
		}
		if len(buf) == 0 {
			rg.End = size
			break
		}

		index := bytes.Index(buf, lineSep)
		if index != -1 {
			appendOutput(buf[:index])
			rg.End = endOff + int64(index+len(lineSep))
		} else {
			appendOutput(buf)
			endOff = min(size, endOff+bufSize)
		}
	}
	return rg, slices.Concat(output...), nil
}
