package bsbf

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"slices"
	"strconv"
)

func getLine(data []byte, off int, lineSep []byte) (int, []byte) {
	i := bytes.Index(data[off:], lineSep)
	if i >= 0 {
		return off + i + len(lineSep), data[off : off+i]
	}
	return len(data), data[off:]
}

func sortSource(path string, data []byte, lineSep []byte, sizeFile int, keySep KeySepFunc, cmpFunc CmpFunc) error {
	s := sorter{
		cmpFunc: cmpFunc,
		lineSep: lineSep,
		keySep:  keySep,
		path:    path,
		data:    data,
	}

	err := s.sliceSort(sizeFile)
	if err != nil {
		return err
	}

	return s.mergeSort()
}

type sorter struct {
	cmpFunc CmpFunc
	keySep  KeySepFunc
	data    []byte
	lineSep []byte
	path    string

	tmpCacheKeyIndex []keyIndex
	sortFiles        []*os.File
}

type keyIndex struct {
	Range Range
	Key   []byte
}

func (k *keyIndex) Encode(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, int64(len(k.Key)))
	if err != nil {
		return err
	}

	_, err = w.Write(k.Key)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, k.Range.Begin)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, k.Range.End)
	if err != nil {
		return err
	}

	return nil
}

func (k *keyIndex) Decode(r io.Reader) error {
	var count int64
	err := binary.Read(r, binary.BigEndian, &count)
	if err != nil {
		return err
	}
	k.Key = make([]byte, count)
	_, err = io.ReadFull(r, k.Key)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &k.Range.Begin)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &k.Range.End)
	if err != nil {
		return err
	}

	return nil
}

func (s *sorter) insert(r Range, k []byte) {
	s.tmpCacheKeyIndex = append(s.tmpCacheKeyIndex, keyIndex{
		Range: r,
		Key:   k,
	})
}

func (s *sorter) saveFile() error {
	if len(s.tmpCacheKeyIndex) == 0 {
		return nil
	}

	slices.SortFunc(s.tmpCacheKeyIndex, func(a, b keyIndex) int {
		return s.cmpFunc(a.Key, b.Key)
	})

	tmpFile := s.path + ".index.gz." + strconv.FormatInt(int64(len(s.sortFiles)), 10)
	w, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	enc := gzip.NewWriter(w)
	for _, k := range s.tmpCacheKeyIndex {
		err = k.Encode(enc)
		if err != nil {
			return err
		}
	}
	err = enc.Close()
	if err != nil {
		return err
	}

	err = w.Sync()
	if err != nil {
		return err
	}
	s.tmpCacheKeyIndex = s.tmpCacheKeyIndex[:0]
	s.sortFiles = append(s.sortFiles, w)
	return nil
}

func (s *sorter) sliceSort(sizeFile int) error {
	var (
		line []byte
	)
	for off := 0; off != len(s.data); {
		beginOff := off
		off, line = getLine(s.data, off, s.lineSep)
		if len(s.lineSep) == 0 {
			break
		}
		r := Range{
			Begin: int64(beginOff),
			End:   int64(off),
		}

		k, _ := s.keySep(line)

		s.insert(r, k)

		if len(s.tmpCacheKeyIndex) >= sizeFile {
			err := s.saveFile()
			if err != nil {
				return err
			}
		}
	}

	err := s.saveFile()
	if err != nil {
		return err
	}
	return nil
}

func (s *sorter) mergeSort() error {
	if len(s.sortFiles) == 0 {
		return nil
	}
	defer func() {
		for _, file := range s.sortFiles {
			file.Close()
			os.Remove(file.Name())
		}
	}()

	w, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer w.Close()

	keyIndexes := make([]*keyIndex, len(s.sortFiles))
	readers := make([]*gzip.Reader, 0, len(s.sortFiles))

	for i, f := range s.sortFiles {
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		reader, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		readers = append(readers, reader)

		var kv keyIndex
		err = kv.Decode(reader)
		if err != nil {

			return err
		}
		keyIndexes[i] = &kv
	}

	for {
		var (
			minKey   []byte
			minIndex = -1
		)
		for i, kv := range keyIndexes {
			if kv == nil {
				continue
			}
			if minKey == nil {
				minKey = kv.Key
				minIndex = i
			} else if s.cmpFunc(kv.Key, minKey) == -1 {
				minKey = kv.Key
				minIndex = i
			}
		}
		if minIndex == -1 || keyIndexes[minIndex] == nil {
			break
		}

		r := keyIndexes[minIndex].Range
		_, err = w.Write(s.data[r.Begin:r.End])
		if err != nil {
			return err
		}

		var kv keyIndex
		err := kv.Decode(readers[minIndex])
		if err != nil {
			if err == io.EOF {
				keyIndexes[minIndex] = nil
				continue
			}
			return err
		}
		keyIndexes[minIndex] = &kv
	}

	return nil
}
