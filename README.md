
Golang Adaptive Radix Tree
==========================

[![GoDoc](https://godoc.org/github.com/jayloop/radix?status.svg)](http://godoc.org/github.com/jayloop/radix) [![Go Report](https://goreportcard.com/badge/github.com/jayloop/radix)](https://goreportcard.com/report/github.com/jayloop/radix)

Radix is a fully adaptive radix tree written in Go. A radix tree is space-optimized prefix tree (trie). An adaptive radix tree uses nodes of variable sizes to minimize memory use. The radix (r) for this implementation is 256.

For details on radix tree see https://en.wikipedia.org/wiki/Radix_tree

For background on adaptive radix trees, see the original ART paper https://db.in.tum.de/~leis/papers/ART.pdf

The tree is safe for concurrent use, and batch operations using multiple goroutines scales pretty well. Especially, concurrent lookups are look-free making them blazingly fast. 

For performance reasons the tree uses multiple allocators, each working on it's own memory arena and having its own recycling data. The number of allocators is configurable, but defaults to runtime.NumCPU(). To safeguard concurrent reads, the allocators are used for all operations, including Lookups.

As an example 49 million URL:s, with an average length of 60 bytes, can be indexed using 10.21 bytes per key (including the 55 bit value pointer). This way billions of keys can be indexed in memory on a single server. 

## Features

* Safe for concurrent use
* Lock-free lookups
* Extremely memory efficient
* No garbage collection overhead
* Very fast to persist to disc
* Snappy compression of memory blocks written to disc
* Keeps data sorted
* Ordered iteration
* Ordered prefix iteration
* Min/max searches
* Prefix counting

## Limitations

1. To save memory, this tree does not store the full keys. It only stores as much information as needed to distinguish between the different keys. It is assumed the full keys are retrievable by the user. In the typical use case they would be stored on a permanent storage.

2. Max 55 bits are available for storing values associated with the inserted keys. The values could be offsets to permanent storage or maybe compressed memory pointers.

3. The tree accepts any []byte data as keys, but in case you want to use range or min/max searches, the key data needs to be binary comparable. For uint64 all you need to do is store them in big-endian order. For int64 you also flip the sign bit. ASCII strings are trivial, but floating point numbers requires more transformations. UTF8 is rather complex (see golang.org/x/text/collate).

4. When indexing keys of variable length (like strings) the requirement is that no key may be the prefix of another key. A simple solution for this is to append a 0 byte on all keys (given 0 is not used within the keys).

## Installation

    go get -u github.com/jayloop/radix

## Setup

````go
trie := radix.NewTree(nil)    
````
To specify the number of allocators.

````go
trie := radix.NewTree(&radix.Options{
    NumAllocators: 1,
})
````

## Operations
    
Since the tree doesn't store the full keys, you need to verify that the value returned points to the correct key. It could otherwise be another key sharing a prefix with the lookup key.

````go
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

````go
found, op := trie.PrepareUpdate(key)
````

If v is non-zero we must fetch the key associated with it and compare it to the insert key.

````go
if found {
    op.FetchedKey = fetchKeyFromSomeStorage(op.FetchedKey[:0], op.Value)
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

`````go
found, op := trie.PrepareDelete(key)
`````
`````go
if !found {
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

`````go
a := trie.GetAllocator()
defer trie.ReleaseAllocator(a)

a.Lookup(key)
a.PrepareUpdate(key)
a.PrepareDelete(key)
// ...

`````

## Iterators and range scans

To iterate the whole index in lexicographic order:

`````go
i := NewIterator(trie, nil)
for i.Next() {
    v := i.Value()
    // ... 
}
`````

To iterate part of the index, or make range searches, pass the prefix to NewIterator

`````go
i := NewIterator(trie, []byte("prefix"))
`````