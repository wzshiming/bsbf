package bsbf

import (
	"bytes"
)

func Compare(a, b []byte) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return bytes.Compare(a, b)
}

func KeySeparator(sep []byte) func(a []byte) ([]byte, []byte) {
	return func(a []byte) ([]byte, []byte) {
		index := bytes.Index(a, sep)
		if index != -1 {
			return a[:index], a[index+len(sep):]
		}
		return a, nil
	}
}
