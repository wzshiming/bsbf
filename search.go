package bsbf

import (
	"bytes"
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

		var c *searchCacheItem
		if b.cache != nil {
			c, _ = b.cache[off]
		}

		if c == nil {
			rg, content, err := seekLine(b.data, b.size, b.bufSize, b.lineSep, off)
			if err != nil {
				return Range{}, nil, nil, false, err
			}

			k, v, ok := b.keySepFunc(content)
			if !ok {
				continue
			}

			c = &searchCacheItem{
				Range: rg,
				Key:   k,
				Value: v,
			}

			if b.cache != nil {
				b.cache[off] = c
			}
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

func seekLine(data slicer, size int64, bufSize int64, lineSep []byte, off int64) (Range, []byte, error) {
	rg := Range{
		Begin: -1,
		End:   -1,
	}

	if off == 0 {
		rg.Begin = off
	}

	var endOffset int64 = -1

	for beginOff := off; rg.Begin == -1; {
		prevBeginOff := beginOff
		beginOff = max(0, beginOff-bufSize)
		limitBufSize := min(bufSize, off-beginOff)

		buf, err := data.Slice(int(beginOff), int(beginOff+limitBufSize))
		if err != nil {
			return Range{}, nil, err
		}
		index := bytes.LastIndex(buf[:limitBufSize], lineSep)
		if index != -1 {
			rg.Begin = prevBeginOff - (limitBufSize - int64(index)) + int64(len(lineSep))
		} else if beginOff == 0 {
			rg.Begin = beginOff
		}
	}

	for endOff := off; rg.End == -1; {
		buf, err := data.Slice(int(endOff), int(endOff+bufSize))
		if err != nil {
			return Range{}, nil, err
		}
		if len(buf) == 0 {
			rg.End = size
			endOffset = size
			break
		}

		index := bytes.Index(buf, lineSep)
		if index != -1 {
			rg.End = endOff + int64(index+len(lineSep))
			endOffset = rg.End - int64(len(lineSep))
		} else {
			endOff = min(size, endOff+bufSize)
		}
	}

	line, err := data.Slice(int(rg.Begin), int(endOffset))
	if err != nil {
		return Range{}, nil, err
	}
	return rg, line, nil
}
