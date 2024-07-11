package bsbf

import (
	"bytes"
	"testing"
)

func TestNextAndPrevious(t *testing.T) {
	data := []byte(`ab 1
cd 2
ef 3`)

	b := NewBSBF(WithData(data))
	iter1, ok, err := b.Search([]byte("ab"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok")
	}
	if !bytes.Equal(iter1.Value(), []byte("1")) {
		t.Fatal("expected 1")
	}

	iter2, ok := iter1.Next()
	if !ok {
		t.Fatal("expected ok")
	}
	if !bytes.Equal(iter2.Value(), []byte("2")) {
		t.Fatal("expected 2")
	}

	iter3, ok := iter2.Next()
	if !ok {
		t.Fatal("expected ok")
	}
	if !bytes.Equal(iter3.Value(), []byte("3")) {
		t.Fatal("expected 3")
	}

	_, ok = iter3.Next()
	if ok {
		t.Fatal("expected not ok")
	}

	iter2Again, ok := iter3.Previous()
	if !ok {
		t.Fatal("expected ok")
	}
	if !bytes.Equal(iter2Again.Value(), []byte("2")) {
		t.Fatal("expected 2")
	}

	iter1Again, ok := iter2Again.Previous()
	if !ok {
		t.Fatal("expected ok")
	}
	if !bytes.Equal(iter1Again.Value(), []byte("1")) {
		t.Fatal("expected 1")
	}

	_, ok = iter1Again.Previous()
	if ok {
		t.Fatal("expected not ok")
	}
}
