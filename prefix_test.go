package radix

import (
	"bytes"
	"log"
	"testing"
)

func TestStorePrefix(t *testing.T) {
	buf := make([]uint64, 1)
	prefix := []byte("hellohi")
	storePrefixSafe(buf, prefix)
	log.Printf("Storing %0x as %x\n", prefix, buf)
	p, slots := loadPrefixSafe(nil, buf, len(prefix))
	assertEqual(t, string(p), string(prefix))
	assertEqual(t, slots, 1)

	buf = make([]uint64, 2)
	prefix = []byte("hellohiii")
	storePrefixSafe(buf, prefix)
	log.Printf("Storing %0x as %x\n", prefix, buf)
	p, slots = loadPrefixSafe(nil, buf, len(prefix))
	assertEqual(t, string(p), string(prefix))
	assertEqual(t, slots, 2)
}

func TestStoreAppendLargePrefix(t *testing.T) {
	prefix := bytes.Repeat([]byte("hello"), 100)
	slots := (len(prefix) + 15) >> 3
	dst := make([]uint64, slots+1)
	storePrefix(dst, prefix)
	assertTrue(t, int(dst[0]) == len(prefix))
	assertTrue(t, dst[len(dst)-1] == 0)

	var (
		buf []byte
		s   int
	)
	buf, s = appendPrefix(buf, dst, 255)
	assertTrue(t, s == slots)
	assertTrue(t, string(buf) == string(prefix))
}
