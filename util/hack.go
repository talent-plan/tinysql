package util

import (
	"reflect"
	"unsafe"
)

// MutableString can be used as string via string(MutableString) without performance loss.
type MutableString string

// String converts slice to MutableString without copy.
// The MutableString can be converts to string without copy.
// Use it at your own risk.
func String(b []byte) (s MutableString) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// Slice converts string to slice without copy.
// Use at your own risk.
func Slice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}
