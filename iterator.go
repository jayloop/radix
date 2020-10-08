package radix

import (
	"sync"
	"sync/atomic"
)

const (
	defaultPrefetchLength = 256
)

type position struct {
	node uint64
	k    int
	more bool
}

// TODO: We need to do range scans (from, to) and not just prefix scans. As an option or through a separate RangeIterator.

// An Iterator is used to scan all or parts of a Tree in lexicographical order.
// Iterators run concurrently with updates or deletes. Items inserted or deleted during the iteration may or may not be seen.
// Iterator is not safe for concurrent use.
type Iterator struct {
	idx          *Tree
	value        uint64
	stack        []position
	prefetched   []uint64
	pp           int
	searchPrefix []byte
	resumeAt     []byte
	done         bool
}

var iteratorPool = sync.Pool{
	New: func() interface{} { return &Iterator{} },
}

// NewIterator returns an Iterator on tree for listing all items having the prefix (prefix may be nil to list all items).
// To recycle the Iterator struct, call Iterator.Close once you are done with it.
func NewIterator(tree *Tree, prefix []byte) *Iterator {
	i := iteratorPool.Get().(*Iterator)
	i.idx = tree
	i.Reset(prefix)
	return i
}

// Next prepares the next item for reading with the Value method. It
// returns true on success, or false if there are no more values.
//
// Every call to Value, even the first one, must be preceded by a call to Next.
func (i *Iterator) Next() bool {

	// if we have any prefetched items left use that
	if i.pp < len(i.prefetched) {
		i.value = i.prefetched[i.pp]
		i.pp++
		return true
	}
	if i.done {
		return false
	}

	// no more prefetched items, we need to scan
	i.stack = i.stack[:0]

	// 1. reserve the current epoch, we need an allocator for this
	// updates to the index during the iteration may or may not be returned
	alloc := i.idx.GetAllocator()
	alloc.startOp()

	// 2. seek to start
	var raw uint64
	if len(i.searchPrefix) > 0 {
		raw, _ = i.idx.partialSearch(i.searchPrefix)
	} else {
		raw = atomic.LoadUint64(&i.idx.root)
	}
	if raw == 0 {
		alloc.endOp()
		i.idx.ReleaseAllocator(alloc)
		i.done = true
		return false
	}
	if isLeaf(raw) {
		alloc.endOp()
		i.idx.ReleaseAllocator(alloc)
		i.pp = 0
		i.value = getLeafValue(raw)
		i.done = true
		return true
	}

	// 3. seek to the partial key where we last left of, while building stack
	// we traverse i.resumeAt to reposition raw & nextChild
	nextChild := 0
preSearchLoop:
	for k := 0; k < len(i.resumeAt); k++ {
		_, node, count, prefixLen := explodeNode(raw)
		data := i.idx.getNodeData(node)
		if prefixLen > 0 {
			var prefixSlots int
			if prefixLen == 255 {
				prefixLen = int(data[0])
				prefixSlots = ((prefixLen + 15) >> 3)
			} else {
				prefixSlots = ((prefixLen + 7) >> 3)
			}
			data = data[prefixSlots:]
			k += prefixLen
			if k >= len(i.resumeAt) {
				break preSearchLoop
			}
		}

		char := i.resumeAt[k]
		if count >= fullAllocFrom {
			// check a fixed size node-256
			a := atomic.LoadUint64(&data[int(char)])
			if a != 0 && byte(a) == char {
				if isLeaf(a) {
					nextChild = int(char)
					break preSearchLoop
				}
				// child node found
				// since it's a node-256 we need to check if there are any more children in this node
				var next int
				for next = int(char) + 1; next < count; next++ {
					if atomic.LoadUint64(&data[next]) != 0 {
						break
					}
				}
				// go down into this node, saving the current position on the stack
				i.stack = append(i.stack, position{raw, next, next < count})
				raw = a
				continue preSearchLoop
			}
			// if no node was found, it means that this subtree might have been deleted since last iteration
			// what are our options in this case ?
			break preSearchLoop
		} else {
			// scan a variable sized node
			for l := range data[:count] {
				a := atomic.LoadUint64(&data[l])
				if byte(a) == char {
					if isLeaf(a) {
						nextChild = l
						break preSearchLoop
					}
					// go down into this node, saving the current position on the stack
					i.stack = append(i.stack, position{raw, l + 1, l+1 < count})
					raw = a
					continue preSearchLoop
				}
			}
			// if no node was found, it means that this subtree might have been deleted since last iteration
			// what are our options in this case ?
			break
		}
	}

	// 4. continue the scan, adding leaves to the prefetched array
	// raw and nextChild is now positioned on the next item to prefetch
	i.prefetched = i.prefetched[:0]
searchLoop:
	for {
		_, node, count, prefixLen := explodeNode(raw)
		data := i.idx.getNodeData(node)
		if prefixLen > 0 {
			prefixSlots := ((prefixLen + 7) >> 3)
			if prefixLen == 255 {
				prefixLen = int(data[0])
				prefixSlots = ((prefixLen + 15) >> 3)
			}
			data = data[prefixSlots:]
		}

		// iterate over node children, they are guaranteed to be stored in order 👍
		for k := nextChild; k < count; k++ {
			a := atomic.LoadUint64(&data[k])
			if a != 0 {
				if isLeaf(a) {
					// add it the the list
					i.prefetched = append(i.prefetched, getLeafValue(a))
				} else {
					next := k + 1
					if count >= fullAllocFrom {
						// since it's a node-256 we need to check if there are any more non-zero children in this node
						for ; next < count; next++ {
							if atomic.LoadUint64(&data[next]) != 0 {
								break
							}
						}
					}
					// go down into this node, saving the current position on the stack (even if there are children left to examine)
					i.stack = append(i.stack, position{raw, next, next < count})
					raw = a
					nextChild = 0
					continue searchLoop
				}
			}
		}
		// we have arrived at the bottom of the tree (a node with only leafs)!

		// if stack is empty we have exhausted the whole search space
		if len(i.stack) == 0 {
			i.done = true
			break
		}
		// if we have prefetched enough values, save the position and abort the scan
		if len(i.prefetched) > defaultPrefetchLength {
			i.storeResumePosition()
			break
		}
		// pop from stack
		var pos position
		pos, i.stack = i.stack[len(i.stack)-1], i.stack[:len(i.stack)-1]
		raw = pos.node
		nextChild = pos.k
	}

	alloc.endOp()
	i.idx.ReleaseAllocator(alloc)

	if len(i.prefetched) > 0 {
		i.value = i.prefetched[0]
		i.pp = 1
		return true
	}

	return false
}

func (i *Iterator) storeResumePosition() {
	// first trim consecutive items without any more children from end of stack
	var j int
	for j = len(i.stack) - 1; j > 0; j-- {
		if i.stack[j].more {
			break
		}
	}
	i.stack = i.stack[:j+1]

	// traverse stack from start to end to build the partial key we should continue from
	i.resumeAt = i.resumeAt[:0]
	for j, pos := range i.stack {
		key, node, _, prefixLen := explodeNode(pos.node)
		data := i.idx.getNodeData(node)
		if j > 0 {
			i.resumeAt = append(i.resumeAt, key)
		}
		var prefixSlots int
		if prefixLen > 0 {
			i.resumeAt, prefixSlots = appendPrefix(i.resumeAt, data, prefixLen)
		}
		// for the last stack item, we need to add the nextChild too
		if j == len(i.stack)-1 {
			a := atomic.LoadUint64(&data[prefixSlots+pos.k])
			i.resumeAt = append(i.resumeAt, byte(a))
		}
	}
}

// Value returns the value stored at the current item.
func (i *Iterator) Value() uint64 {
	return i.value
}

// Reset restarts the iteration with the given prefix. This permits reusing Iterators.
func (i *Iterator) Reset(prefix []byte) {
	i.searchPrefix = append(i.searchPrefix[:0], prefix...)
	i.prefetched = i.prefetched[:0]
	i.resumeAt = i.resumeAt[:0]
	i.pp = 0
	i.stack = i.stack[:0]
	i.value = 0
	i.done = false
}

// Close returns the Iterator the the pool. It must not be used after this call.
func (i *Iterator) Close() {
	iteratorPool.Put(i)
}
