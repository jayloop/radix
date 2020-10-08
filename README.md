
# Golang Adaptive Radix Tree

[![GoDoc](https://godoc.org/github.com/jayloop/radix?status.svg)](http://godoc.org/github.com/jayloop/radix) [![Go Report](https://goreportcard.com/badge/github.com/jayloop/radix)](https://goreportcard.com/report/github.com/jayloop/radix)

Radix is a fully adaptive radix tree written in Go. A radix tree is space-optimized prefix tree (trie). An adaptive radix tree uses nodes of variable sizes to minimize memory use. The radix (r) for this implementation is 256.

For details on radix tree see https://en.wikipedia.org/wiki/Radix_tree

For background on adaptive radix trees, see the original ART paper https://db.in.tum.de/~leis/papers/ART.pdf

The tree is safe for concurrent use. Especially, concurrent lookups are look-free making them blazingly fast. 

For performance reasons the tree uses multiple allocators, each working on it's own memory arena and having it's own recycling data. The number of allocators is configurable, but defaults to runtime.NumCPU(). To safeguard concurrent reads, the allocators are used for all operations, including Lookups.

As an example 17 million URL:s, with an average length of 60 bytes, can be indexed using approx 11 bytes per key (including the 55 bit value pointer). This way billions of keys can be indexed in memory on a single server. 

## Limitations

1. To save memory, this tree does not store the full keys in the leaves. It only stores as much information as needed to distinguish between the different keys. It is assumed the full keys are retrievable by the user. In the typical use case they would be stored on a permanent storage.

2. Max 55 bits are available for storing values associated with the inserted keys. The values could be offsets to permanent storage or compressed memory pointers.

3. The 0 value can not be stored with a key (since a 0 return value means key not found).

4. The tree accepts any []byte data as keys, but in case you want to use range or min/max searches, the key data needs to be binary comparable. For uint64 all you need to do is store them in big-endian order. For int64 the sign bit also needs to be flipped. ASCII strings are trivial, but floating point numbers requires more transformations. UTF8 is rather complex (see golang.org/x/text/collate).

5. When indexing keys of variable length (like strings) the requirement is that no key may be the prefix of another key. A simple solution for this is to append a 0 byte on all keys (given 0 is not used within the keys).

## Installation

    go get -u github.com/jayloop/radix

## Setup

````
trie := radix.NewTree(nil)    
````

````
trie := radix.NewTree(&radix.Options{
    NumAllocators: 1,
})
````

## Operations
    
Since the tree doesn't store the full keys, you need to verify that the value returned points to the correct key. It could otherwise be another key sharing a prefix with the lookup key.

````
key := []byte("foo")
v := trie.Lookup(key)
if v == 0 {
    // nothing found
    return NotFound
}
record := fetchRecordFromSomeStorage(v)
if string(key) != string(record.Key) {
    // mismatch
    return NotFound
}
// Match!
// do something with record
````


Update and delete operations are a multi-step process.

````
v, op := trie.PrepareUpdate(key)
````

If v is non-zero we must fetch the key associated with it and compare it to the insert key.

````
if v != 0 {
    op.FetchedKey = fetchKeyFromSomeStorage(op.FetchedKey[:0], v)
    if string(key) == string(op.FetchedKey) {
        // this is an update
        // do some update logic or abort if we only wanted to insert
        op.Match = true        
    } else {
        // this is an insert
        // do some insert logic or abort if we only wanted to update
    }
}
if !op.Finalize(3) {
    // write conflict, we need to restart with a new PrepareUpdate
}
````

Delete operations are similar:

`````
v, op := trie.PrepareDelete(key)
`````
`````
if v == 0 {
    // nothing found
    return
}
op.FetchedKey = fetchKeyFromSomeStorage(op.FetchedKey[:0], v)
if string(key) != string(op.FetchedKey) {
    // mismatch
    op.Abort()
    return
}
if !op.Finalize() {
    // write conflict, we need to restart with a new PrepareDelete
}
`````

## Using allocators

To speed up batch data operations, you can require an allocator and use it for all operations, then release it.

    trie := NewRadixTree(nil)
    a := trie.GetAllocator()
    ...
    a.Lookup()
    a.PrepareUpdate(...)
    a.PrepareDelete(...)
    ...
    trie.ReleaseAllocator(a)

## Iterators and range scans

To iterate the whole index in lexicographic order

    i := NewIterator(trie, nil)
    for i.Next() {
        v := i.Value()
        ... 
    }

To iterate part of the index, or make range searches, pass the prefix to NewIterator

    i := NewIterator(trie, PREFIX)