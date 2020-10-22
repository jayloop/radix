package fuzzing

import (
	"bytes"
	"fmt"

	"github.com/jayloop/radix"
)

// Fuzz builds a tree based on the input data which is treated as a list of \n separated keys.
// It tries to cover as much code as possible to increase the chance of finding bugs!
func Fuzz(data []byte) int {
	// Parse data
	var db [][]byte
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			// no more keys
			break
		}
		if i == 0 {
			// zero length key, return -1 to not add this to corpus
			return -1
		}
		if bytes.IndexByte(data[:i], 0) >= 0 {
			// keys should not contain 0 byte
			return -1
		}
		key := make([]byte, i+1)
		copy(key, data[:i])
		db = append(db, key)
		data = data[i+1:]
	}

	// Build the tree
	trie := radix.NewTree(&radix.Options{
		NumAllocators: 1,
	})
	for i, key := range db {
	S:
		found, op := trie.PrepareUpdate(key)
		if found {
			if string(key) == string(db[op.Value]) {
				// duplicate key in database, abort
				return -1
			} else {
				op.Match = false
				op.FetchedKey = db[op.Value]
			}
		}
		if !op.Finalize(uint64(i)) {
			goto S
		}
	}

	// Lookup all items
	for i, key := range db {
		v, found := trie.Lookup(key)
		if !found {
			panic(fmt.Sprintf("Key %s not found", key))
		}
		if v != uint64(i) {
			panic(fmt.Sprintf("Wrong result for lookup %s", key))
		}
	}

	// Iterate it
	i := radix.NewIterator(trie, nil)
	for i.Next() {
		i.Value()
	}

	// Count it
	c := radix.NewCounter(trie)
	c.Count(nil)

	// Delete it
	for _, key := range db {
	D:
		found, op := trie.PrepareDelete(key)
		if !found {
			panic(fmt.Sprintf("Key %s not found during delete", key))
		}
		if string(key) != string(db[op.Value]) {
			panic(fmt.Sprintf("Wrong key (%s) found while deleting %s", db[op.Value], key))
		}
		if !op.Finalize() {
			goto D
		}
	}
	// Close it so we don't leek goroutines
	trie.Close()

	return 1
}
