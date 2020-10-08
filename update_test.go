package radix

import (
	"fmt"
	"strings"
	"testing"
)

func TestLongPrefix(t *testing.T) {
	trie := NewTree(nil)

	shortKey := "abcdefghijklmnopqrstuvwxyz"
	longKey := strings.Repeat(shortKey, 10)

	_, op := trie.PrepareUpdate([]byte(longKey + "def"))
	assertTrue(t, op.Finalize(1))
	fmt.Printf("%v\n", op)
	assertEqual(t, trie.Lookup([]byte(longKey+"def")), uint64(1))

	_, op = trie.PrepareUpdate([]byte(longKey + "abc"))
	op.Match = false
	op.FetchedKey = []byte(longKey + "def")
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(2))
	assertEqual(t, trie.Lookup([]byte(longKey+"abc")), uint64(2))
}

func TestRootPrefixMissUpdate(t *testing.T) {
	trie := NewTree(nil)
	// insert a first leaf value
	_, op := trie.PrepareUpdate([]byte("hellao"))
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(1))
	assertEqual(t, trie.Lookup([]byte("hellao")), uint64(1))

	// insert a second one, should return first leaf as value
	v, op := trie.PrepareUpdate([]byte("hellaroy"))
	assertEqual(t, v, uint64(1))
	op.Match = false
	op.FetchedKey = []byte("hellao")
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(2))

	assertEqual(t, trie.Lookup([]byte("hellao")), uint64(1))
	assertEqual(t, trie.Lookup([]byte("hellaroy")), uint64(2))

	// now we should have a root node with prefix "hell" and children "o" and "r"
	// insert a new leaf only partly matching prefix which should trigger a NODE_PREFIX_MISS operation
	// this should insert a new root node with prefix "hel" and leaf child "i" + node child "l" (no prefix) with leafs "o" and "r"
	v, op = trie.PrepareUpdate([]byte("helios"))
	assertEqual(t, v, uint64(0))
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(3))

	assertEqual(t, trie.Lookup([]byte("helios")), uint64(3))
	assertEqual(t, trie.Lookup([]byte("hellaroy")), uint64(2))
	assertEqual(t, trie.Lookup([]byte("hellao")), uint64(1))
}

func TestPrefixMissUpdate(t *testing.T) {
	trie := NewTree(nil)
	// insert a first leaf value
	_, op := trie.PrepareUpdate([]byte("hello"))
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(1))
	assertEqual(t, trie.Lookup([]byte("hello")), uint64(1))

	// insert a second one
	_, op = trie.PrepareUpdate([]byte("greetings"))
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(2))

	// insert another starting with g => root: [h g], g: node, prefix "r" [e a]
	v, op := trie.PrepareUpdate([]byte("gratulations"))
	assertEqual(t, v, uint64(2))
	op.Match = false
	op.FetchedKey = []byte("greetings")
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(3))

	// trigger a NODE_PREFIX_MISS
	v, op = trie.PrepareUpdate([]byte("glorius"))
	assertEqual(t, v, uint64(0))
	fmt.Printf("%v\n", op)
	assertTrue(t, op.Finalize(4))

	assertEqual(t, trie.Lookup([]byte("hello")), uint64(1))
	assertEqual(t, trie.Lookup([]byte("greetings")), uint64(2))
	assertEqual(t, trie.Lookup([]byte("gratulations")), uint64(3))
	assertEqual(t, trie.Lookup([]byte("glorius")), uint64(4))
}

func TestLeafRootUpdate(t *testing.T) {
	trie := NewTree(nil)

	// insert a first leaf value
	v, op := trie.PrepareUpdate([]byte("hello"))
	fmt.Printf("%v\n", op)
	assertEqual(t, v, uint64(0))
	assertEqual(t, op.Finalize(1234), true)
	assertEqual(t, trie.Lookup([]byte("hello")), uint64(1234))

	// update it
	v, op = trie.PrepareUpdate([]byte("hello"))
	assertEqual(t, v, uint64(1234))
	fmt.Printf("%v\n", op)
	op.Match = true
	assertEqual(t, op.Finalize(12345), true)
	assertEqual(t, trie.Lookup([]byte("hello")), uint64(12345))

	// add a new key with common prefix, should convert root into a node-2 with prefix
	v, op = trie.PrepareUpdate([]byte("here"))
	assertEqual(t, v, uint64(12345))
	fmt.Printf("%v\n", op)
	op.Match = false
	op.FetchedKey = []byte("hello")
	assertEqual(t, op.Finalize(987), true)
	assertEqual(t, trie.Lookup([]byte("here")), uint64(987))
	assertEqual(t, trie.Lookup([]byte("hello")), uint64(12345))
}

func TestUpdate(t *testing.T) {
	trie := NewTree(nil)

	// insert a first leaf value
	v, op := trie.PrepareUpdate([]byte("hello"))
	fmt.Printf("%v\n", op)
	assertEqual(t, v, uint64(0))
	updated := op.Finalize(1234)
	assertEqual(t, updated, true)
	assertEqual(t, trie.Lookup([]byte("hello")), uint64(1234))
	assertEqual(t, trie.Lookup([]byte("hellohi")), uint64(1234))
	assertEqual(t, trie.Lookup([]byte("grotesk")), uint64(0))

	// insert a second leaf value (should convert root to a node-2 with 0 prefix)
	v, op = trie.PrepareUpdate([]byte("grotesk"))
	assertEqual(t, v, uint64(0))
	fmt.Printf("%v\n", op)
	assertEqual(t, op.Finalize(5678), true)
	assertEqual(t, trie.Lookup([]byte("hello")), uint64(1234))
	assertEqual(t, trie.Lookup([]byte("grotesk")), uint64(5678))

	// insert a third leaf value (should convert root to a node-3 with 0 prefix)
	v, op = trie.PrepareUpdate([]byte("frog"))
	assertEqual(t, v, uint64(0))
	fmt.Printf("%v\n", op)
	assertEqual(t, op.Finalize(909), true)
	assertEqual(t, trie.Lookup([]byte("frog")), uint64(909))

	// should convert root leaf "hello" into a "h" node-2 with prefix "e", and childs "r" and "l", v is for hello node
	v, op = trie.PrepareUpdate([]byte("here"))
	assertEqual(t, v, uint64(1234))
	op.FetchedKey = []byte("hello")
	op.Match = false
	fmt.Printf("%v\n", op)
	assertEqual(t, op.Finalize(123), true)
	assertEqual(t, trie.Lookup([]byte("here")), uint64(123))

	v, op = trie.PrepareUpdate([]byte("here"))
	assertEqual(t, v, uint64(123))
	op.Match = true
	fmt.Printf("%v\n", op)
	assertEqual(t, op.Finalize(900), true)
	assertEqual(t, trie.Lookup([]byte("here")), uint64(900))
}
