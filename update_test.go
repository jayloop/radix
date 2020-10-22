package radix

import (
	"strings"
	"testing"
)

func TestLongPrefix(t *testing.T) {
	trie := NewTree(nil)

	shortKey := "abcdefghijklmnopqrstuvwxyz"
	longKey := strings.Repeat(shortKey, 10)

	_, op := trie.PrepareUpdate([]byte(longKey + "def"))
	assertTrue(t, op.Finalize(1))
	v, _ := trie.Lookup([]byte(longKey + "def"))
	assertEqual(t, v, uint64(1))

	_, op = trie.PrepareUpdate([]byte(longKey + "abc"))
	op.Match = false
	op.FetchedKey = []byte(longKey + "def")
	assertTrue(t, op.Finalize(2))
	v, _ = trie.Lookup([]byte(longKey + "abc"))
	assertEqual(t, v, uint64(2))
}

func TestRootPrefixMissUpdate(t *testing.T) {
	trie := NewTree(nil)
	// insert a first leaf value
	_, op := trie.PrepareUpdate([]byte("hellao"))
	assertTrue(t, op.Finalize(1))
	v, _ := trie.Lookup([]byte("hellao"))
	assertEqual(t, v, uint64(1))

	// insert a second one, should return first leaf as value
	var found bool
	found, op = trie.PrepareUpdate([]byte("hellaroy"))
	assertEqual(t, found, true)
	assertEqual(t, op.Value, uint64(1))
	op.Match = false
	op.FetchedKey = []byte("hellao")
	assertTrue(t, op.Finalize(2))

	v, _ = trie.Lookup([]byte("hellao"))
	assertEqual(t, v, uint64(1))
	v, _ = trie.Lookup([]byte("hellaroy"))
	assertEqual(t, v, uint64(2))

	// now we should have a root node with prefix "hell" and children "o" and "r"
	// insert a new leaf only partly matching prefix which should trigger a NODE_PREFIX_MISS operation
	// this should insert a new root node with prefix "hel" and leaf child "i" + node child "l" (no prefix) with leafs "o" and "r"
	found, op = trie.PrepareUpdate([]byte("helios"))
	assertEqual(t, found, false)
	assertTrue(t, op.Finalize(3))

	v, _ = trie.Lookup([]byte("helios"))
	assertEqual(t, v, uint64(3))
	v, _ = trie.Lookup([]byte("hellaroy"))
	assertEqual(t, v, uint64(2))
	v, _ = trie.Lookup([]byte("hellao"))
	assertEqual(t, v, uint64(1))
}

func TestPrefixMissUpdate(t *testing.T) {
	trie := NewTree(nil)
	// insert a first leaf value
	_, op := trie.PrepareUpdate([]byte("hello"))
	assertTrue(t, op.Finalize(1))
	v, _ := trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(1))

	// insert a second one
	_, op = trie.PrepareUpdate([]byte("greetings"))
	assertTrue(t, op.Finalize(2))

	// insert another starting with g => root: [h g], g: node, prefix "r" [e a]
	var found bool
	found, op = trie.PrepareUpdate([]byte("gratulations"))
	assertEqual(t, found, true)
	assertEqual(t, op.Value, uint64(2))
	op.Match = false
	op.FetchedKey = []byte("greetings")
	assertTrue(t, op.Finalize(3))

	// trigger a NODE_PREFIX_MISS
	found, op = trie.PrepareUpdate([]byte("glorius"))
	assertEqual(t, found, false)
	assertTrue(t, op.Finalize(4))

	v, _ = trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(1))
	v, _ = trie.Lookup([]byte("greetings"))
	assertEqual(t, v, uint64(2))
	v, _ = trie.Lookup([]byte("gratulations"))
	assertEqual(t, v, uint64(3))
	v, _ = trie.Lookup([]byte("glorius"))
	assertEqual(t, v, uint64(4))
}

func TestLeafRootUpdate(t *testing.T) {
	trie := NewTree(nil)

	// insert a first leaf value
	found, op := trie.PrepareUpdate([]byte("hello"))
	assertEqual(t, found, false)
	assertEqual(t, op.Finalize(1234), true)
	v, _ := trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(1234))

	// update it
	found, op = trie.PrepareUpdate([]byte("hello"))
	assertEqual(t, op.Value, uint64(1234))
	op.Match = true
	assertEqual(t, op.Finalize(12345), true)
	v, _ = trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(12345))

	// add a new key with common prefix, should convert root into a node-2 with prefix
	found, op = trie.PrepareUpdate([]byte("here"))
	assertEqual(t, op.Value, uint64(12345))
	op.Match = false
	op.FetchedKey = []byte("hello")
	assertEqual(t, op.Finalize(987), true)
	v, _ = trie.Lookup([]byte("here"))
	assertEqual(t, v, uint64(987))
	v, _ = trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(12345))
}

func TestUpdate(t *testing.T) {
	trie := NewTree(nil)

	// insert a first leaf value
	found, op := trie.PrepareUpdate([]byte("hello"))
	assertEqual(t, found, false)
	updated := op.Finalize(1234)
	assertEqual(t, updated, true)
	v, _ := trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(1234))
	v, _ = trie.Lookup([]byte("hellohi"))
	assertEqual(t, v, uint64(1234))
	v, _ = trie.Lookup([]byte("grotesk"))
	assertEqual(t, v, uint64(0))

	// insert a second leaf value (should convert root to a node-2 with 0 prefix)
	found, op = trie.PrepareUpdate([]byte("grotesk"))
	assertEqual(t, found, false)
	assertEqual(t, op.Finalize(5678), true)
	v, _ = trie.Lookup([]byte("hello"))
	assertEqual(t, v, uint64(1234))
	v, _ = trie.Lookup([]byte("grotesk"))
	assertEqual(t, v, uint64(5678))

	// insert a third leaf value (should convert root to a node-3 with 0 prefix)
	found, op = trie.PrepareUpdate([]byte("frog"))
	assertEqual(t, found, false)
	assertEqual(t, op.Finalize(909), true)
	v, _ = trie.Lookup([]byte("frog"))
	assertEqual(t, v, uint64(909))

	// should convert root leaf "hello" into a "h" node-2 with prefix "e", and childs "r" and "l", v is for hello node
	found, op = trie.PrepareUpdate([]byte("here"))
	assertEqual(t, op.Value, uint64(1234))
	op.FetchedKey = []byte("hello")
	op.Match = false
	assertEqual(t, op.Finalize(123), true)
	v, _ = trie.Lookup([]byte("here"))
	assertEqual(t, v, uint64(123))

	found, op = trie.PrepareUpdate([]byte("here"))
	assertEqual(t, op.Value, uint64(123))
	op.Match = true
	assertEqual(t, op.Finalize(900), true)
	v, _ = trie.Lookup([]byte("here"))
	assertEqual(t, v, uint64(900))
}
