package radix

import (
	"reflect"
	"unsafe"
)

//go:linkname memmove runtime.memmove
func memmove(to, from unsafe.Pointer, n uintptr)

func byteSliceFromUint64(buf []uint64) []byte {
	// reflection magic... turn the []uint64 slice into a []byte slice
	sh := &reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&buf[0])),
		Len:  len(buf) * 8,
		Cap:  len(buf) * 8,
	}
	return *(*[]byte)(unsafe.Pointer(sh))
}

func storePrefix(dst []uint64, prefix []byte) {
	if len(prefix) >= 255 {
		dst[0] = uint64(len(prefix))
		dst = dst[1:]
	}
	memmove(unsafe.Pointer(&dst[0]), unsafe.Pointer(&prefix[0]), uintptr(len(prefix)))
}

// appendPrefix returns the number of slots used to store the prefix at src
func appendPrefix(dst []byte, src []uint64, length int) ([]byte, int) {
	var (
		slots int
	)
	switch length {
	case 0:
		return dst, 0
	case 255:
		// prefixLen == 255 means it's >= 255, the real length is for simplicity stored in the next 8 byte slot (the unsafe version uses only 2 bytes for length)
		length = int(src[0])
		slots = (length + 15) >> 3
		src = src[1:]
	default:
		slots = (length + 7) >> 3
	}
	// fix size of dst
	l := len(dst)
	if cap(dst) >= l+length {
		dst = dst[:l+length]
	} else {
		dst2 := make([]byte, l+length)
		copy(dst2, dst)
		dst = dst2
	}
	memmove(unsafe.Pointer(&dst[l]), unsafe.Pointer(&src[0]), uintptr(length))
	return dst, slots
}
