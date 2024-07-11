package bsbf

import (
	"bytes"
)

type searchCacheItem struct {
	Range Range
	Key   []byte
	Value []byte
}

func (b *BSBF) search(key []byte) (Range, []byte, []byte, bool, error) {
	i, j := int64(0), int64(len(b.data))

	for cacheLevel := 0; i < j; cacheLevel++ {
		off := int64(uint(i+j) >> 1)

		var c *searchCacheItem
		if b.cache != nil {
			c, _ = b.cache[off]
		}

		if c == nil {
			begin, end, content := seekLine(b.data, b.bufSize, b.lineSep, off)

			k, v := b.keySepFunc(content)

			rg := Range{
				Begin: begin,
				End:   end,
			}
			c = &searchCacheItem{
				Range: rg,
				Key:   k,
				Value: v,
			}

			if b.cache != nil && (b.cacheLevel < 0 || cacheLevel < b.cacheLevel) {
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

func seekLineBefore(data []byte, bufSize int64, lineSep []byte, off int64) int64 {
	for beginOff := off; beginOff != 0; {
		prevBeginOff := beginOff
		beginOff = max(0, beginOff-bufSize)
		limitBufSize := min(bufSize, off-beginOff)

		buf := data[beginOff : beginOff+limitBufSize]
		index := bytes.LastIndex(buf, lineSep)
		if index != -1 {
			return prevBeginOff - (limitBufSize - int64(index)) + int64(len(lineSep))
		}
	}
	return 0
}

func seekLineAfter(data []byte, bufSize int64, lineSep []byte, off int64) (int64, int64) {
	size := int64(len(data))
	for endOff := off; endOff != size; {
		nextEndOff := min(size, endOff+bufSize)
		if endOff == nextEndOff {
			break
		}
		buf := data[endOff:nextEndOff]
		if len(buf) == 0 {
			break
		}

		index := bytes.Index(buf, lineSep)
		if index != -1 {
			end := endOff + int64(index+len(lineSep))
			return end, end - int64(len(lineSep))
		}
		endOff = nextEndOff
	}
	return size, size
}

func seekLine(data []byte, bufSize int64, lineSep []byte, off int64) (int64, int64, []byte) {
	begin := seekLineBefore(data, bufSize, lineSep, off)
	end, endOffset := seekLineAfter(data, bufSize, lineSep, off)
	return begin, end, data[begin:endOffset]
}
