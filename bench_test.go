package bsbf

import (
	"bytes"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
)

var onceInit sync.Once

func initTestdata() {
	err := os.MkdirAll("./testdata", 0700)
	if err != nil {
		panic(err)
	}
	w, err := os.OpenFile("./testdata/data.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	for i := 0; i < 100000; i++ {
		k := strconv.Itoa(i)
		w.WriteString(strconv.Itoa(i))
		w.Write(keySep)
		v := []byte(k)
		slices.Reverse(v)
		w.Write(v)
		w.Write(lineSep)
	}
}

func BenchmarkBSBF(b *testing.B) {
	onceInit.Do(initTestdata)

	bs := NewBSBF("./testdata/data.txt",
		WithSearchCache(true),
	)

	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		k := []byte(strconv.Itoa(i))
		_, _, val, ok, err := bs.Search(k)
		if err != nil {
			b.Error(err)
		}
		if ok {
			slices.Reverse(k)
			if !bytes.Equal(val, k) {
				b.Errorf("%q != %q", val, k)
			}
		}
	}
}

func BenchmarkParallelBSBF(b *testing.B) {
	onceInit.Do(initTestdata)

	bs := NewBSBF("./testdata/data.txt",
		WithSearchCache(true),
	)

	b.StartTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			k := []byte(strconv.Itoa(i))
			_, _, val, ok, err := bs.Search(k)
			if err != nil {
				b.Error(err)
			}
			if ok {
				slices.Reverse(k)
				if !bytes.Equal(val, k) {
					b.Errorf("%q != %q", val, k)
				}
			}
		}
	})
}
